# Openstack-vm-mover
This tool allow you to migrate a VM on openstack from one project to another.

Openstack API are used, so this script can be executed on native Openstack deployment or on Cloud provider solution like OVHcloud.
For the volume transfer, the VM should be migrated on the same region and the destination project must use the same storage backend.
If the VM have to move on other project in other region, the volume transfers can't be used. A mechanism with a snapshot volume can be used but that can take a long time depending on the volumes size.

## Steps 
1. Snapshot the VM to create an image
2. Upload this image on the other project
3. Use openstack volume transfers to move attached volumes on the other project
4. Create a new instance on the other project based on the image uploaded
5. Attache transfered volumes

## Config file
This tool use a configuration file in yaml:

```yaml
mode: projectToProject
worker_count: 3     # how many worker can work in parallel

servers:
  - name: vm-migration
    networks:
      - UUID: "581fad02-158d-4dc6-81f0-c1ec2794bbec"  # OVHcloud EXT-NET network UUID
      - UUID: "fde034ed-1482-4eb2-ae2c-d305c68cc56b"
        fixedIP: "10.119.128.152"

os_project_source:
  identity_endpoint: https://auth.cloud.ovh.net/v3
  username: 
  password:
  domain_name: Default
  region: SBG5

os_project_destination:
  identity_endpoint: https://auth.cloud.ovh.net/v3
  username: 
  password: 
  domain_name: Default
  region: SBG5
```