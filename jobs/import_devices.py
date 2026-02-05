"""
Nautobot Job: Import Virtual Chassis Devices from CSV
Imports devices with virtual chassis members from Akips CSV export.
"""
import csv
from io import StringIO
from collections import defaultdict
from django.core.exceptions import ValidationError
from nautobot.extras.jobs import Job, TextVar
from nautobot.dcim.models import (
    Device,
    DeviceType,
    Manufacturer,
    Location,
    Rack,
    Platform,
    VirtualChassis,
)
from nautobot.extras.models import Status, Role


class ImportVirtualChassisDevices(Job):
    """
    Import virtual chassis devices from CSV format.
    
    Expected CSV format:
    Device,ID,Model,Software,Serial,MAC Addr,Role,Location
    
    The job will:
    - Group rows by device name (virtual chassis)
    - Check if device already exists
    - Validate existing VC configuration
    - Create new devices with VC members if not exists
    - Parse site from device name
    """
    
    class Meta:
        name = "Import Virtual Chassis Devices"
        description = "Import devices with virtual chassis members from CSV"
        commit_default = False
    
    csv_data = TextVar(
        description="Paste CSV data with header row",
        label="CSV Data"
    )
    
    # Location code to Location name mapping (for Nautobot 2.0+/3.0)
    LOCATION_MAPPING = {
        'arl-art': 'Arlington Tower',
        # Add more mappings as needed
    }
    
    def run(self, data, commit):
        """Main job execution"""
        
        # Parse CSV
        csv_reader = csv.DictReader(StringIO(data['csv_data']))
        rows = list(csv_reader)
        
        if not rows:
            self.log_failure(message="No data found in CSV")
            return
        
        self.log_info(message=f"Parsed {len(rows)} rows from CSV")
        
        # Group rows by device name (virtual chassis)
        devices_data = defaultdict(list)
        for row in rows:
            device_name = row['Device'].strip()
            devices_data[device_name].append(row)
        
        self.log_info(message=f"Found {len(devices_data)} unique devices")
        
        # Process each device
        results = {
            'created': [],
            'validated': [],
            'errors': [],
            'mismatches': []
        }
        
        for device_name, members in devices_data.items():
            try:
                result = self.process_device(device_name, members)
                if result['status'] == 'created':
                    results['created'].append(device_name)
                elif result['status'] == 'validated':
                    results['validated'].append(device_name)
                    if result.get('mismatches'):
                        results['mismatches'].extend(result['mismatches'])
                elif result['status'] == 'error':
                    results['errors'].append({
                        'device': device_name,
                        'error': result['error']
                    })
            except Exception as e:
                self.log_failure(message=f"Error processing {device_name}: {str(e)}")
                results['errors'].append({
                    'device': device_name,
                    'error': str(e)
                })
        
        # Summary
        self.log_success(message=f"Created {len(results['created'])} devices")
        self.log_success(message=f"Validated {len(results['validated'])} existing devices")
        
        if results['mismatches']:
            self.log_warning(message=f"Found {len(results['mismatches'])} mismatches:")
            for mismatch in results['mismatches']:
                self.log_warning(message=f"  {mismatch}")
        
        if results['errors']:
            self.log_failure(message=f"Encountered {len(results['errors'])} errors:")
            for error in results['errors']:
                self.log_failure(message=f"  {error['device']}: {error['error']}")
    
    def process_device(self, device_name, members):
        """Process a single device with its virtual chassis members"""
        
        # Sort members by ID (VC position)
        members = sorted(members, key=lambda x: int(x['ID']))
        
        # Check if device exists
        try:
            device = Device.objects.get(name=device_name)
            return self.validate_existing_device(device, members)
        except Device.DoesNotExist:
            return self.create_new_device(device_name, members)
    
    def validate_existing_device(self, device, members):
        """Validate existing device's virtual chassis configuration"""
        
        self.log_info(message=f"Validating existing device: {device.name}")
        
        mismatches = []
        
        # Check if device has virtual chassis
        if not device.virtual_chassis:
            mismatches.append(f"{device.name}: No virtual chassis configured")
        else:
            vc = device.virtual_chassis
            
            # Get all VC members
            vc_members = Device.objects.filter(virtual_chassis=vc).order_by('vc_position')
            
            # Check member count
            if vc_members.count() != len(members):
                mismatches.append(
                    f"{device.name}: VC member count mismatch - "
                    f"Expected {len(members)}, found {vc_members.count()}"
                )
            
            # Validate each member
            for csv_member in members:
                vc_pos = int(csv_member['ID'])
                csv_serial = csv_member['Serial'].strip()
                
                try:
                    vc_member = vc_members.get(vc_position=vc_pos)
                    
                    # Check serial number
                    if vc_member.serial != csv_serial:
                        mismatches.append(
                            f"{device.name} VC position {vc_pos}: "
                            f"Serial mismatch - Nautobot: {vc_member.serial}, "
                            f"CSV: {csv_serial}"
                        )
                
                except Device.DoesNotExist:
                    mismatches.append(
                        f"{device.name}: Missing VC member at position {vc_pos}"
                    )
        
        if mismatches:
            for mismatch in mismatches:
                self.log_warning(message=mismatch)
        
        return {
            'status': 'validated',
            'mismatches': mismatches
        }
    
    def create_new_device(self, device_name, members):
        """Create new device with virtual chassis"""
        
        self.log_info(message=f"Creating new device: {device_name}")
        
        # Parse location from device name
        location = self.get_location_from_device_name(device_name)
        if not location:
            raise ValidationError(f"Could not determine location for device {device_name}")
        
        # Get the master member (Role = master, typically ID 0)
        master_member = next((m for m in members if m['Role'].strip().lower() == 'master'), members[0])
        
        # Get device type
        device_type = self.get_device_type(master_member['Model'])
        
        # Get or create device role
        device_role, created = Role.objects.get_or_create(
            name='Access',
            defaults={'color': '9e9e9e'}
        )
        if created:
            self.log_success(message="Created Role: Access")
            # Add content types for Device
            from django.contrib.contenttypes.models import ContentType
            device_ct = ContentType.objects.get_for_model(Device)
            device_role.content_types.add(device_ct)
        
        # Get or create platform
        platform, created = Platform.objects.get_or_create(
            name='Juniper_junos',
            defaults={'network_driver': 'juniper_junos'}
        )
        if created:
            self.log_success(message="Created Platform: Juniper_junos")
        
        # Get or create status
        status, created = Status.objects.get_or_create(
            name='Active'
        )
        if created:
            self.log_success(message="Created Status: Active")
            # Add content types for Device
            from django.contrib.contenttypes.models import ContentType
            device_ct = ContentType.objects.get_for_model(Device)
            status.content_types.add(device_ct)
        
        # Parse rack from location if available
        rack = self.get_rack_from_location(master_member.get('Location', ''), location)
        
        # Create virtual chassis
        vc = VirtualChassis.objects.create(
            name=device_name,
            domain=device_name
        )
        self.log_success(message=f"Created virtual chassis: {vc.name}")
        
        # Create master device
        master_device = Device.objects.create(
            name=device_name,
            device_type=device_type,
            role=device_role,
            location=location,
            platform=platform,
            status=status,
            serial=master_member['Serial'].strip(),
            virtual_chassis=vc,
            vc_position=int(master_member['ID']),
            vc_priority=self.get_vc_priority(master_member['Role']),
            rack=rack,
            face='FRONT',
        )
        
        # Set master device on VC
        vc.master = master_device
        vc.save()
        
        self.log_success(message=f"Created master device: {master_device.name} (VC position {master_device.vc_position})")
        
        # Create member devices (non-master)
        for member in members:
            if member['ID'] == master_member['ID']:
                continue  # Skip master, already created
            
            member_name = f"{device_name}-member-{member['ID']}"
            member_device = Device.objects.create(
                name=member_name,
                device_type=device_type,
                role=device_role,
                location=location,
                platform=platform,
                status=status,
                serial=member['Serial'].strip(),
                virtual_chassis=vc,
                vc_position=int(member['ID']),
                vc_priority=self.get_vc_priority(member['Role']),
                rack=rack,
                face='FRONT',
            )
            
            self.log_success(
                message=f"  Created VC member: {member_device.name} "
                f"(position {member_device.vc_position}, role: {member['Role'].strip()})"
            )
        
        return {
            'status': 'created',
            'device': master_device
        }
    
    def get_location_from_device_name(self, device_name):
        """Parse location from device name"""
        # Example: accs-arl-art-1550-1
        # Location code is typically the middle part: arl-art
        
        parts = device_name.split('-')
        if len(parts) >= 3:
            # Try to find location code (e.g., arl-art)
            location_code = f"{parts[1]}-{parts[2]}"
            
            if location_code in self.LOCATION_MAPPING:
                location_name = self.LOCATION_MAPPING[location_code]
                try:
                    location = Location.objects.get(name=location_name)
                    self.log_info(message=f"Mapped {location_code} to location: {location.name}")
                    return location
                except Location.DoesNotExist:
                    self.log_warning(message=f"Location not found: {location_name}")
            else:
                # Try to find location by name matching
                try:
                    location = Location.objects.get(name__icontains=location_code)
                    self.log_info(message=f"Found location by name: {location.name}")
                    return location
                except (Location.DoesNotExist, Location.MultipleObjectsReturned):
                    self.log_warning(
                        message=f"Could not find unique location for code: {location_code}. "
                        f"Add mapping to LOCATION_MAPPING."
                    )
        
        return None
    
    def get_device_type(self, model):
        """Get or create device type"""
        model_upper = model.strip().upper()
        
        try:
            return DeviceType.objects.get(model__iexact=model_upper)
        except DeviceType.DoesNotExist:
            # Create manufacturer if it doesn't exist (assume Juniper for ex* models)
            manufacturer_name = "Juniper"
            if model.lower().startswith('ex'):
                manufacturer_name = "Juniper"
            # Add more logic here if needed for other vendors
            
            manufacturer, created = Manufacturer.objects.get_or_create(
                name=manufacturer_name,
                defaults={'slug': manufacturer_name.lower()}
            )
            
            if created:
                self.log_success(message=f"Created manufacturer: {manufacturer_name}")
            
            # Create device type
            device_type = DeviceType.objects.create(
                manufacturer=manufacturer,
                model=model_upper,
                slug=model_upper.lower().replace(' ', '-')
            )
            
            self.log_success(message=f"Created DeviceType: {model_upper}")
            return device_type
    
    def get_rack_from_location(self, location_str, location):
        """Parse rack from location string"""
        if not location_str:
            return None
        
        # Example: RR-VA-1550-R1R2 RU27
        # Extract rack name (everything before ' RU')
        rack_name = location_str.split(' RU')[0].strip()
        
        if not rack_name:
            return None
        
        try:
            rack = Rack.objects.get(name=rack_name, location=location)
            return rack
        except Rack.DoesNotExist:
            self.log_warning(message=f"Rack not found: {rack_name}")
            return None
        except Rack.MultipleObjectsReturned:
            self.log_warning(message=f"Multiple racks found for name: {rack_name}")
            return None
    
    def get_vc_priority(self, role):
        """Determine VC priority from role"""
        role_lower = role.strip().lower()
        
        if role_lower == 'master':
            return 255
        elif role_lower == 'backup':
            return 128
        else:  # linecard
            return 1


jobs = [ImportVirtualChassisDevices]

from nautobot.extras.jobs import register_jobs
register_jobs(*jobs)
