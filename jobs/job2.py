import csv

from netaddr import IPNetwork, cidr_merge
from nautobot.extras.jobs import Job, ObjectVar, StringVar, TextVar, IPAddressWithMaskVar, BooleanVar
from nautobot.dcim.models import Device, DeviceRole, DeviceType, Manufacturer, Site, VirtualChassis, Region, Platform, Cable
from nautobot.ipam.models import VLAN, Prefix
from nautobot.extras.models import Status, CustomFieldChoice, Tag

class CreateNewSite(Job):
    
    class Meta:
        description = "Create a new Site"
        
    site_name = StringVar(
        description = "Building Name as defined by Campus Planning",
        required = True
    )
    
    building_code = StringVar(
        description = "Building Code as defined by Campus Planning",
        required = True
    )
    
    region = ObjectVar(
        description = "Campus",
        model = Region,
        required = True
    )
    
    status = ObjectVar(
        model=Status,
        required=True
    )
    
    site_prefix = IPAddressWithMaskVar(
        description = "Supernet for the site",
        required=True
    )
    
    site_type = ObjectVar(
        model=CustomFieldChoice,
        required=False,
        query_params={
            'value': [
                'ACADEMIC/ADMIN',
                'RESIDENTIAL',
                'PARKING',
                'SERVICE/UTILITY',
                'SPORT/RECREATION',
                'OTHER'
            ]
        }
    )
    
    def run(self, data, commit):
        status = data['status']
        site = Site(
            name = data['site_name'],
            facility = data['building_code'],
            region = data['region'],
            status = status,
            _custom_field_data={
                'site-type': data['site_type'].value
            }
        )
        site.validated_save()
        self.log_success(obj=site, message=f"Created site {site.name}")
        
        site_prefix = data['site_prefix']
        nb_site_prefix = Prefix(
            network = site_prefix.network,
            prefix_length = site_prefix.prefixlen,
            status = status,
            is_pool = False,
            site = site
        )
        nb_site_prefix.validated_save()
        self.log_success(obj=nb_site_prefix, message=f"Created site prefix {site_prefix}")
        network24s = list(site_prefix.subnet(24, count=64))
        
        user_subnet = cidr_merge(list(network24s[0:8]))
        user_guest_subnet = cidr_merge(list(network24s[8:16]))
        user_wifi_subnet = cidr_merge(list(network24s[16:24]))
        surv_device_subnet = cidr_merge(list(network24s[24:32]))
        ap_mgt_subnet = cidr_merge(list(network24s[32:40]))
        device_subnet = cidr_merge(list(network24s[40:44]))
        oob_mgt_subnet = list(network24s[44:45])
        ib_mgt_subnet = list(network24s[45:46])
        pos_device_subnet = list(network24s[46:47])
        voip_subnet = list(network24s[47:48])
        sec_device_subnet = list(network24s[48:49])
        server_subnet = list(network24s[49:50])
        
        vlans = [
            {'name': 'OOB-MGT', 'vid': '4', 'description': 'Out-of-Band Management', 'subnet': oob_mgt_subnet},
            {'name': 'IB-MGT', 'vid': '5', 'description': 'In-Band Management', 'subnet': ib_mgt_subnet},
            {'name': 'USER', 'vid': '10', 'description': 'User Access for NU Community including PCs, Solstices, etc.', 'subnet': user_subnet},
            {'name': 'USER-GUEST', 'vid': '15', 'description': 'User Guest Wireless Access including eduroam guests', 'subnet': user_guest_subnet},
            {'name': 'USER-WIFI', 'vid': '20', 'description': 'User Wireless Access for NU Community including Solstices', 'subnet': user_wifi_subnet},
            {'name': 'DEVICE', 'vid': '25', 'description': 'Devices including Printers, AV Control, Signage, etc.', 'subnet': device_subnet},
            {'name': 'POS-DEVICE', 'vid': '30', 'description': 'Point-of-Sale Devices', 'subnet': pos_device_subnet},
            {'name': 'SURV-DEVICE', 'vid': '35', 'description': 'Video Surveillance Devices', 'subnet': surv_device_subnet},
            {'name': 'VOIP', 'vid': '40', 'description': 'Voice-over-IP Phones', 'subnet': voip_subnet},
            {'name': 'AP-MGT', 'vid': '45', 'description': 'Wireless Access Point Management', 'subnet': ap_mgt_subnet},
            {'name': 'SEC-DEVICE', 'vid': '50', 'description': 'Physical Access Security Devices (OneCard, etc.)', 'subnet': sec_device_subnet},
            {'name': 'SERVER', 'vid': '55', 'description': 'Server Systems (AD etc.)', 'subnet': server_subnet}
        ]

        for vlan in vlans:
            vlan_subnet = vlan['subnet']
            description = vlan['description']
            vlan = VLAN(
                vid = vlan['vid'], 
                name = vlan['name'],
                status = status,
                site = site,
                description = description      
            )
            if vlan.name == 'VOIP':
                vlan.tags = Tag.objects.get(slug='voice-vlan')
                vlan.validated_save() #Tag isn't being applied to VOIP vlan still. Needs troubleshooting
            vlan.validated_save()
            self.log_success(obj=vlan, message=f"Created vlan name {vlan.name} with vid {vlan.vid}.")
            
            vlan_prefix = Prefix(
                network = str(vlan_subnet[0].network),
                prefix_length = str(vlan_subnet[0].prefixlen),
                status = status,
                is_pool = True,
                site = site,
                vlan = vlan,
                description = description
            )
            vlan_prefix.validated_save()
            self.log_success(obj=vlan_prefix, message=f"Created prefix {vlan_prefix} and attached to vlan {vlan.name}.")

class CreateVirtualChassis(Job):
  
    class Meta:
      description = "Create Juniper Virtual Chassis."
  
    hostname = StringVar(
        description="Hostname of the Virtual Chassis",
        required=True
    )

    device_type = ObjectVar(
        description="Hardware model",
        model=DeviceType
    )
    
    members = TextVar(
        description="Enter members data as CSV. Member role options are 'routing-engine' or 'linecard'.",
        default="serial,member_role,member_position",
        required=True
    )

    role = ObjectVar(
        model=DeviceRole,
        required=True,
        query_params={
            'name':['Access','Aggregation']
        }
    )
    
    site = ObjectVar(
        model=Site,
        required=True
    )
    
    status = ObjectVar(
        model=Status,
        required=True,
    )

    def run(self, data, commit):
        vc_name = data['hostname']
        site = data['site']
        dev_status = data['status']
        dev_role = data['role']
        virtual_chassis = VirtualChassis(name=vc_name)      
        virtual_chassis.validated_save()
        self.log_success(obj=virtual_chassis, message=f"Created Virtual Chassis {vc_name}")

        print(dir(data['device_type']))
        if data['device_type'].manufacturer.name == "Juniper":
            platform = Platform.objects.get(name="Juniper_junos")
        elif data['device_type'].manufacturer.name == "Aruba":
            platform = Platform.objects.get(name="AOSCX")
        else:
            self.log_info(obj=virtual_chassis, message="Unable to set platform")
        
        mems_csv = data['members'].splitlines()
        mems_list = csv.DictReader(mems_csv, delimiter=',')
        mems = list(mems_list)
        for member in mems:
            # Master member check
            if member["member_position"] == "0":
              position = ""
              master_member = True
            else:
              position = f':{member["member_position"]}'
              master_member = False
            dev_type = data['device_type']
            vc_member = Device(
                name=vc_name + position,
                site=site,
                serial=member["serial"],
                status=dev_status,
                device_type=dev_type,
                device_role=dev_role,
                virtual_chassis=virtual_chassis,
                vc_position=member['member_position'],
                platform=platform
            )
           
            # Juniper role check
            if member["member_role"] == "routing-engine":
                vc_member.vc_priority = 1
            
            vc_member.validated_save()
            self.log_success(obj=vc_member, message=f"Created VC member switch {vc_member}")
            # Master member check
            if master_member == True:
              virtual_chassis.master = vc_member
              virtual_chassis.validated_save()
              self.log_success(obj=vc_member, message=f"Assigned VC member {vc_member} as the VC Master")
              continue
            else:
                rename_interfaces = list(vc_member.interfaces.all())
                dedup_intfs = ["irb", "irb.5", "ae0", "vme", "vlan", "lo0", "mgmt"]
                for intf in rename_interfaces:
                    if any(x in intf.name for x in dedup_intfs):
                        intf.delete()
                    else:
                        if data['device_type'].manufacturer.name == "Juniper":
                            intf.name = intf.name.replace("-0/", f"-{member['member_position']}/")
                        elif data['device_type'].manufacturer.name == "Aruba":
                            intf.name = intf.name.replace("1/", f"{int(member['member_position']) + 1}/")
                        intf.validated_save()
            
          

class CreateCables(Job):

    """
    TODO: 
        Handle existing cable updates, done need to test
            May want handle cable already exists  (e.g. case where no need to update), would be faster
        Handle creating quads if don't exist
    """
    
    class Meta:
        description = "Import cable sheet"
        name = "Create Cables"
        
    cable_data = TextVar(
        description="Cable data in csv format",
        default="""switch,sw_port,ppanel,ppanel_port,quad,quad_port\ntest-accs-1:1,48,test-site-ppanel-1,49,test_quad,A""",
        required=True
    )

    override = BooleanVar(
        description="Replace existing cables",
        default=False
    )

    def run(self, data, commit):
        cable_csv = data['cable_data'].splitlines()
        cable_list = csv.DictReader(cable_csv, delimiter=',')
        cables = list(cable_list)
        override = data['override']

        status = Status.objects.get(name="Connected")
        #self.log_info(status, message=status.name)

        count = 0
        for cable in cables:
            #self.log_info(None, message=str(cable))
            #self.log_info(None, message=str(cable['switch']))              
            device_obj = Device.objects.get(name=cable['switch']) #get device for entry
            interface_obj = None
            for item in device_obj.interfaces.all():
                if item.name.endswith(f"/0/{cable['sw_port']}"): #could be slow for interface lookup?
                    interface_obj = item
                    #self.log_success(obj=interface_obj, message=f"Matched interface {interface_obj.name}")
                    break
            if interface_obj == None: #should log here when no interface match
                continue

            #get ppanel
            ppanel = Device.objects.get(name=cable['ppanel'])
            ppanel_frontport = ppanel.frontports.get(name=cable['ppanel_port'])
            ppanel_rearport = ppanel.rearports.get(name=cable['ppanel_port'])
            #self.log_success(obj=ppanel_port, message=f"Matched ppanel port {ppanel_port.name}")

            #get quad
            quad = Device.objects.get(name=cable['quad'])
            quad_frontport = quad.frontports.get(name=cable['quad_port'])
            #self.log_success(obj=quad_frontport, message=f"Matched quad_port {quad_frontport.name}")

            #delete any existing cables on endpoints
            if override:
                #self.log_info(None, message=str(self.override))
                #self.log_info(None, message="Testing")
                if interface_obj.cable != None:
                    interface_obj.cable.delete()
                    interface_obj.refresh_from_db()

                ppanel_frontport.refresh_from_db() #must refresh first incase cable was deleted by prev operation
                if ppanel_frontport.cable != None:
                    ppanel_frontport.cable.delete()
                    ppanel_frontport.refresh_from_db() #refresh after to clean fields

                ppanel_rearport.refresh_from_db()
                if ppanel_rearport.cable != None:
                    ppanel_rearport.cable.delete()
                    ppanel_rearport.refresh_from_db()

                quad_frontport.refresh_from_db()
                if quad_frontport.cable != None:
                    quad_frontport.cable.delete()
                    quad_frontport.refresh_from_db()
                    
                        
                
            #create new cable, sw to ppanel
            sw_ppanel_cable = Cable(termination_a_id=interface_obj.id, termination_a_type=interface_obj._content_type,
                                    termination_b_id=ppanel_frontport.id, termination_b_type=ppanel_frontport._content_type,
                                    status=status)
            sw_ppanel_cable.validated_save()
            count +=1

            #create new cable, ppanel to quad
            ppanel_quad_cable = Cable(termination_a_id=ppanel_rearport.id, termination_a_type=ppanel_rearport._content_type,
                                      termination_b_id=quad_frontport.id, termination_b_type=quad_frontport._content_type,
                                      status=status)

            ppanel_quad_cable.validated_save()
            count += 1
        self.log_success(None, message=f"{count} cables successfully imported.")
                    
        
      
