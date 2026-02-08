"""
Nautobot Job to import devices from AKIPS CSV export.
Handles virtual chassis creation and device management.
"""
import csv
import re
from io import StringIO

from django.core.exceptions import ObjectDoesNotExist
from nautobot.dcim.models import (
    Device,
    DeviceRole,
    DeviceType,
    Manufacturer,
    Platform,
    Region,
    Site,
    VirtualChassis,
)
from nautobot.extras.jobs import Job, FileVar, BooleanVar


class AkipsDeviceImport(Job):
    """
    Import devices from AKIPS CSV export with virtual chassis support.
    """

    class Meta:
        name = "AKIPS Device Import"
        description = "Import devices from AKIPS CSV file with virtual chassis management"
        has_sensitive_variables = False

    csv_file = FileVar(
        description="AKIPS CSV file containing device information",
        required=True
    )
    
    create_missing_objects = BooleanVar(
        description="Create missing DeviceTypes, Regions, and Sites if not found",
        default=False
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stats = {
            'devices_created': 0,
            'devices_updated': 0,
            'devices_skipped': 0,
            'virtual_chassis_created': 0,
            'virtual_chassis_updated': 0,
            'errors': [],
            'warnings': [],
            'serial_mismatches': [],
            'vc_mismatches': []
        }
        self.create_missing = False

    def parse_facility_code(self, device_name):
        """
        Extract facility code from device name by checking each part sequentially.
        
        Examples:
            accs-arl-art-1550-1:
                Skip 'accs', check 'arl' -> if match found, return it
                Otherwise check 'art' -> if match found, return it
                Otherwise return None (numbers start)
            
            accs-ho-414-1:
                Skip 'accs', check 'ho' -> if match found, return it
                Otherwise hit '414' (numbers), return None
        
        Logic: Split by '-', skip first part (device role), 
        then try each subsequent part until we find a site match or hit numbers.
        """
        parts = device_name.split('-')
        
        if len(parts) < 3:
            return None
        
        # Skip the first part (device role like 'accs')
        for part in parts[1:]:
            # Stop when we encounter a part that starts with a digit
            if part and part[0].isdigit():
                return None
            
            # Try to find a site with this facility code
            facility_code = part.upper()
            site = self._check_site_exists(facility_code)
            if site:
                return facility_code
        
        return None
    
    def _check_site_exists(self, facility_code):
        """Check if a site exists with the given facility code."""
        try:
            return Site.objects.get(facility=facility_code)
        except ObjectDoesNotExist:
            # Try case-insensitive search
            return Site.objects.filter(facility__iexact=facility_code).first()
        except Site.MultipleObjectsReturned:
            return Site.objects.filter(facility=facility_code).first()

    def extract_device_role(self, device_name):
        """
        Extract device role from device name.
        
        Examples:
            accs-arl-art-1550-1 -> Access
            dist-ho-414-1 -> Distribution
            core-ho-414-1 -> Core
        """
        role_mapping = {
            'accs': 'Access',
            'dist': 'Distribution',
            'core': 'Core',
            'edge': 'Edge',
            'aggr': 'Aggregation',
        }
        
        first_part = device_name.split('-')[0].lower()
        return role_mapping.get(first_part, 'Access')  # Default to Access

    def get_or_create_device_role(self, role_name):
        """Get or create a DeviceRole."""
        try:
            return DeviceRole.objects.get(name=role_name)
        except ObjectDoesNotExist:
            self.log_warning(f"DeviceRole '{role_name}' not found. Please create it manually.")
            return None

    def get_or_create_device_type(self, model_name):
        """Get or create a DeviceType with flexible matching."""
        manufacturer, _ = Manufacturer.objects.get_or_create(
            name="Juniper",
            defaults={"slug": "juniper"}
        )
        
        # Normalize the model name from CSV (e.g., "ex4300-48p" -> "EX4300-48P")
        normalized_model = model_name.upper()
        
        # Try multiple lookup strategies
        lookup_attempts = [
            model_name,  # Exact match as-is
            normalized_model,  # Uppercase version
            f"Juniper {normalized_model}",  # With "Juniper" prefix
            f"Juniper {model_name}",  # With "Juniper" prefix, original case
        ]
        
        # Try each lookup strategy
        for attempt in lookup_attempts:
            try:
                device_type = DeviceType.objects.get(model=attempt, manufacturer=manufacturer)
                self.log_debug(f"Found DeviceType: {device_type.model}")
                return device_type
            except ObjectDoesNotExist:
                continue
            except DeviceType.MultipleObjectsReturned:
                # If multiple found, use the first one
                device_type = DeviceType.objects.filter(model=attempt, manufacturer=manufacturer).first()
                self.log_warning(f"Multiple DeviceTypes found for '{attempt}', using: {device_type.model}")
                return device_type
        
        # If still not found, try case-insensitive search
        device_type = DeviceType.objects.filter(
            model__iexact=normalized_model,
            manufacturer=manufacturer
        ).first()
        
        if device_type:
            self.log_debug(f"Found DeviceType via case-insensitive search: {device_type.model}")
            return device_type
        
        # Try with "Juniper" prefix case-insensitive
        device_type = DeviceType.objects.filter(
            model__iexact=f"Juniper {normalized_model}",
            manufacturer=manufacturer
        ).first()
        
        if device_type:
            self.log_debug(f"Found DeviceType via case-insensitive search: {device_type.model}")
            return device_type
        
        # If create_missing is enabled, create it
        if self.create_missing:
            # Create with "Juniper" prefix to match your convention
            full_model_name = f"Juniper {normalized_model}"
            slug = full_model_name.lower().replace(' ', '-')
            device_type = DeviceType.objects.create(
                manufacturer=manufacturer,
                model=full_model_name,
                slug=slug
            )
            self.log_success(f"Created DeviceType: {full_model_name}")
            return device_type
        else:
            self.log_warning(
                f"DeviceType for model '{model_name}' not found (tried: {', '.join(lookup_attempts)}). "
                "Enable 'create_missing_objects' or create it manually."
            )
            return None

    def get_platform(self):
        """Get or create the Juniper_junos platform."""
        try:
            return Platform.objects.get(slug="juniper-junos")
        except ObjectDoesNotExist:
            try:
                return Platform.objects.get(name="Juniper_junos")
            except ObjectDoesNotExist:
                platform = Platform.objects.create(
                    name="Juniper_junos",
                    slug="juniper-junos",
                    manufacturer=Manufacturer.objects.get(name="Juniper")
                )
                self.log_success("Created Platform: Juniper_junos")
                return platform

    def find_site_by_facility_code(self, facility_code):
        """Find a site by facility code."""
        if not facility_code:
            return None, None
        
        site = self._check_site_exists(facility_code)
        if site:
            return site, site.region
        return None, None

    def parse_csv(self, csv_content):
        """Parse CSV content and return grouped devices."""
        csv_file = StringIO(csv_content.decode('utf-8'))
        reader = csv.DictReader(csv_file)
        
        # Group devices by device name (virtual chassis)
        device_groups = {}
        
        for row in reader:
            device_name = row['Device'].strip()
            
            if device_name not in device_groups:
                device_groups[device_name] = []
            
            device_groups[device_name].append({
                'device_name': device_name,
                'member_id': int(row['ID']),
                'model': row['Model'].strip(),
                'software': row['Software'].strip(),
                'serial': row['Serial'].strip(),
                'mac_addr': row['MAC Addr'].strip(),
                'role': row['Role'].strip(),  # master/backup/linecard
                'location': row.get('Location', '').strip()
            })
        
        # Sort members by ID within each group
        for device_name in device_groups:
            device_groups[device_name].sort(key=lambda x: x['member_id'])
        
        return device_groups

    def process_virtual_chassis(self, device_name, members):
        """Process a virtual chassis and its member devices."""
        self.log_info(f"\n{'='*60}")
        self.log_info(f"Processing: {device_name} ({len(members)} members)")
        self.log_info(f"{'='*60}")
        
        # Extract facility code and find site/region
        facility_code = self.parse_facility_code(device_name)
        self.log_info(f"Extracted facility code: {facility_code}")
        
        site, region = self.find_site_by_facility_code(facility_code)
        
        if not site:
            warning = f"No site found for facility code '{facility_code}' (device: {device_name})"
            self.log_warning(warning)
            self.stats['warnings'].append(warning)
        else:
            self.log_success(f"Matched to site: {site.name} (Region: {region.name if region else 'None'})")
        
        # Get common attributes
        device_role_name = self.extract_device_role(device_name)
        device_role = self.get_or_create_device_role(device_role_name)
        platform = self.get_platform()
        
        if not device_role:
            error = f"Cannot proceed without DeviceRole for {device_name}"
            self.log_failure(error)
            self.stats['errors'].append(error)
            self.stats['devices_skipped'] += len(members)
            return
        
        # Find or create master device first
        master_member = next((m for m in members if m['role'].lower() == 'master'), members[0])
        
        # Check if virtual chassis already exists
        existing_vc = VirtualChassis.objects.filter(master__name=device_name).first()
        
        if not existing_vc:
            # Check if any member device exists
            existing_device = Device.objects.filter(name=device_name).first()
            
            if existing_device:
                # Device exists but no VC - check if we need to create VC
                self.log_info(f"Device {device_name} exists without virtual chassis")
                
                # Verify serial number
                if existing_device.serial != master_member['serial']:
                    mismatch = {
                        'device': device_name,
                        'member_id': master_member['member_id'],
                        'expected_serial': master_member['serial'],
                        'actual_serial': existing_device.serial
                    }
                    self.stats['serial_mismatches'].append(mismatch)
                    self.log_warning(
                        f"Serial mismatch for {device_name}: "
                        f"Expected {master_member['serial']}, found {existing_device.serial}"
                    )
                
                # Create virtual chassis for existing device
                if len(members) > 1:
                    existing_vc = self.create_virtual_chassis(
                        device_name, existing_device, members, site, region
                    )
                else:
                    self.log_info(f"Single-member chassis, no VC needed for {device_name}")
                    self.stats['devices_updated'] += 1
            else:
                # Create new device and virtual chassis
                self.log_info(f"Creating new device: {device_name}")
                master_device = self.create_device(
                    device_name, master_member, device_role, platform, site, region
                )
                
                if master_device and len(members) > 1:
                    existing_vc = self.create_virtual_chassis(
                        device_name, master_device, members, site, region
                    )
                elif master_device:
                    self.log_info(f"Single-member chassis, no VC needed for {device_name}")
        else:
            self.log_info(f"Virtual chassis exists for {device_name}")
            self.verify_and_update_virtual_chassis(existing_vc, members, device_role, platform, site, region)

    def create_device(self, device_name, member_data, device_role, platform, site, region):
        """Create a new device."""
        device_type = self.get_or_create_device_type(member_data['model'])
        
        if not device_type:
            error = f"Cannot create device {device_name} without DeviceType"
            self.log_failure(error)
            self.stats['errors'].append(error)
            self.stats['devices_skipped'] += 1
            return None
        
        try:
            # Create member device name (e.g., accs-arl-art-1550-1-0)
            member_device_name = f"{device_name}-{member_data['member_id']}"
            
            device = Device.objects.create(
                name=member_device_name,
                device_role=device_role,
                device_type=device_type,
                platform=platform,
                serial=member_data['serial'],
                site=site,
                region=region,
                comments=f"Software: {member_data['software']}\nMAC: {member_data['mac_addr']}\nLocation: {member_data['location']}"
            )
            
            self.log_success(f"Created device: {member_device_name} (Serial: {member_data['serial']})")
            self.stats['devices_created'] += 1
            return device
            
        except Exception as e:
            error = f"Failed to create device {device_name}: {str(e)}"
            self.log_failure(error)
            self.stats['errors'].append(error)
            self.stats['devices_skipped'] += 1
            return None

    def create_virtual_chassis(self, vc_name, master_device, members, site, region):
        """Create a virtual chassis with members."""
        try:
            # Create the virtual chassis
            vc = VirtualChassis.objects.create(
                name=vc_name,
                master=master_device,
                domain=vc_name
            )
            
            self.log_success(f"Created Virtual Chassis: {vc_name}")
            
            # Get common attributes for all members
            device_role = master_device.device_role
            platform = master_device.platform
            
            # Create/update all member devices
            for member_data in members:
                member_device_name = f"{vc_name}-{member_data['member_id']}"
                
                # Check if member device exists
                member_device = Device.objects.filter(name=member_device_name).first()
                
                if member_device:
                    # Update existing member
                    member_device.virtual_chassis = vc
                    member_device.vc_position = member_data['member_id']
                    member_device.vc_priority = 1 if member_data['role'].lower() == 'master' else (
                        1 if member_data['role'].lower() == 'backup' else None
                    )
                    member_device.save()
                    self.log_info(f"Updated member: {member_device_name}")
                else:
                    # Create new member
                    device_type = self.get_or_create_device_type(member_data['model'])
                    if not device_type:
                        continue
                    
                    member_device = Device.objects.create(
                        name=member_device_name,
                        device_role=device_role,
                        device_type=device_type,
                        platform=platform,
                        serial=member_data['serial'],
                        site=site,
                        region=region,
                        virtual_chassis=vc,
                        vc_position=member_data['member_id'],
                        vc_priority=1 if member_data['role'].lower() == 'master' else (
                            1 if member_data['role'].lower() == 'backup' else None
                        ),
                        comments=f"Software: {member_data['software']}\nMAC: {member_data['mac_addr']}\nLocation: {member_data['location']}"
                    )
                    self.log_success(f"Created member: {member_device_name} (Position: {member_data['member_id']})")
                    self.stats['devices_created'] += 1
            
            # Update master reference if needed
            master_device_name = f"{vc_name}-{members[0]['member_id']}"
            actual_master = Device.objects.get(name=master_device_name)
            if vc.master != actual_master:
                vc.master = actual_master
                vc.save()
            
            self.stats['virtual_chassis_created'] += 1
            return vc
            
        except Exception as e:
            error = f"Failed to create virtual chassis {vc_name}: {str(e)}"
            self.log_failure(error)
            self.stats['errors'].append(error)
            return None

    def verify_and_update_virtual_chassis(self, vc, members, device_role, platform, site, region):
        """Verify and update existing virtual chassis."""
        self.log_info(f"Verifying virtual chassis: {vc.name}")
        
        # Get all member devices
        vc_members = Device.objects.filter(virtual_chassis=vc).order_by('vc_position')
        
        # Check member count
        if vc_members.count() != len(members):
            warning = (
                f"Member count mismatch for {vc.name}: "
                f"Expected {len(members)}, found {vc_members.count()}"
            )
            self.log_warning(warning)
            self.stats['vc_mismatches'].append({
                'vc': vc.name,
                'issue': 'member_count',
                'expected': len(members),
                'actual': vc_members.count()
            })
        
        # Verify each member
        for member_data in members:
            member_device_name = f"{vc.name}-{member_data['member_id']}"
            member_device = Device.objects.filter(name=member_device_name).first()
            
            if not member_device:
                self.log_warning(f"Member device not found: {member_device_name}")
                # Create missing member
                device_type = self.get_or_create_device_type(member_data['model'])
                if device_type:
                    member_device = Device.objects.create(
                        name=member_device_name,
                        device_role=device_role,
                        device_type=device_type,
                        platform=platform,
                        serial=member_data['serial'],
                        site=site,
                        region=region,
                        virtual_chassis=vc,
                        vc_position=member_data['member_id'],
                        vc_priority=1 if member_data['role'].lower() == 'master' else (
                            1 if member_data['role'].lower() == 'backup' else None
                        ),
                        comments=f"Software: {member_data['software']}\nMAC: {member_data['mac_addr']}\nLocation: {member_data['location']}"
                    )
                    self.log_success(f"Created missing member: {member_device_name}")
                    self.stats['devices_created'] += 1
                continue
            
            # Verify serial number
            if member_device.serial != member_data['serial']:
                mismatch = {
                    'device': member_device_name,
                    'member_id': member_data['member_id'],
                    'expected_serial': member_data['serial'],
                    'actual_serial': member_device.serial
                }
                self.stats['serial_mismatches'].append(mismatch)
                self.log_warning(
                    f"Serial mismatch for {member_device_name}: "
                    f"Expected {member_data['serial']}, found {member_device.serial}"
                )
            
            # Verify VC position
            if member_device.vc_position != member_data['member_id']:
                self.log_warning(
                    f"VC position mismatch for {member_device_name}: "
                    f"Expected {member_data['member_id']}, found {member_device.vc_position}"
                )
        
        self.stats['virtual_chassis_updated'] += 1

    def run(self, data, commit):
        """Main job execution."""
        self.log_info("Starting AKIPS Device Import")
        
        # Set the create_missing flag from input data
        self.create_missing = data.get('create_missing_objects', False)
        
        try:
            # Parse CSV
            csv_content = data['csv_file'].read()
            device_groups = self.parse_csv(csv_content)
            
            self.log_info(f"Found {len(device_groups)} device(s) in CSV")
            
            # Process each device/virtual chassis
            for device_name, members in device_groups.items():
                self.process_virtual_chassis(device_name, members)
            
            # Print summary
            self.log_info("\n" + "="*60)
            self.log_info("IMPORT SUMMARY")
            self.log_info("="*60)
            self.log_info(f"Devices Created: {self.stats['devices_created']}")
            self.log_info(f"Devices Updated: {self.stats['devices_updated']}")
            self.log_info(f"Devices Skipped: {self.stats['devices_skipped']}")
            self.log_info(f"Virtual Chassis Created: {self.stats['virtual_chassis_created']}")
            self.log_info(f"Virtual Chassis Updated: {self.stats['virtual_chassis_updated']}")
            
            # Report mismatches
            if self.stats['serial_mismatches']:
                self.log_warning(f"\n{len(self.stats['serial_mismatches'])} Serial Number Mismatches:")
                for mismatch in self.stats['serial_mismatches']:
                    self.log_warning(
                        f"  {mismatch['device']} (Member {mismatch['member_id']}): "
                        f"Expected {mismatch['expected_serial']}, "
                        f"Found {mismatch['actual_serial']}"
                    )
            
            if self.stats['vc_mismatches']:
                self.log_warning(f"\n{len(self.stats['vc_mismatches'])} Virtual Chassis Mismatches:")
                for mismatch in self.stats['vc_mismatches']:
                    self.log_warning(f"  {mismatch}")
            
            if self.stats['warnings']:
                self.log_warning(f"\n{len(self.stats['warnings'])} Warning(s) occurred:")
                for warning in self.stats['warnings'][:10]:  # Show first 10
                    self.log_warning(f"  {warning}")
            
            if self.stats['errors']:
                self.log_failure(f"\n{len(self.stats['errors'])} Error(s) occurred:")
                for error in self.stats['errors'][:10]:  # Show first 10
                    self.log_failure(f"  {error}")
            
            self.log_success("\nImport completed!")
            
        except Exception as e:
            self.log_failure(f"Job failed with error: {str(e)}")
            raise


jobs = [AkipsDeviceImport]