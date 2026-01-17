# Deployment

_The system is currently deployed on the [SCD cloud](https://cloud.stfc.ac.uk/)._
_In theory it can be deployed on any cloud infrastructure but it will require_
_changes before this will work._

_It is not yet a production-grade, HA system._

There are several prerequisites steps required before deployment can begin. Please
read [them here](./prerequisites.md).

## Ansible

Ansible playbooks in `<repo_root>/ansible-docker/playbooks` control the deployment
of the system. All commands in this section assume that the current working directory
is `infra/ansible-docker`.

### Networking

Create the private VM network:

```sh
ansible-playbook playbooks/cloud/private_network_create.yml
```

Create a node for Traefik (also acts as an SSH jump node):

```sh
ansible-playbook \
  -e openstack_cloud_name=<cloud_name> \
  -e openstack_key_name=<ssh_key> \
  -e vm_config_file=$PWD/inventories/qa/group_vars/traefik.yml
  playbooks/cloud/vm_create.yml
```

where _cloud\_name_ and _ssh\_key_ are described in the [prerequisites section](./prerequisites.md#openstack-api--vm-credentials).
Take a note of the new node ip address and create a new inventory file:

```sh
cp inventory-sample.yml inventory.yml
```

Fill in the new Traefik ip address and deploy Traefik:

```sh
ansible-playbook -i inventory.yml playbooks/deploy-traefik.yml
```

Once deployed check the Traefik dashboard is available at `https://<domain>/traefik/dashboard/.`
The passwords are in Keeper.

### Services

Now we deploy the remaining services. The deployment order is important as some
services depend on others being available. Each service has a single VM with the
exception of Superset that hosts multiple instances on a single node.

First create the VMs:

```sh
for svc in keycloak lakekeeper trino elt; do
  ansible-playbook \
    -e openstack_cloud_name=<cloud_name> \
    -e openstack_key_name=<ssh_key> \
    -e vm_config_file=$PWD/inventories/qa/group_vars/$svc.yml
    playbooks/cloud/vm_create.yml
done
# Now Superset
ansible-playbook \
  -e openstack_cloud_name=<cloud_name> \
  -e openstack_key_name=<ssh_key> \
  -e vm_config_file=$PWD/inventories/qa/group_vars/superset/all.yml
  playbooks/cloud/vm_create.yml
```

Gather the new ip addresses of each VM and fill in the appropriate section of the new `inventory.yml` created above.

Now deploy the services:

```sh
for svc in traefik keycloak lakekeeper trino elt superset; do
  ansible-playbook -i inventory.yml playbooks/deploy-$svc.yml
done
```

Superset should be available at `https://<domain>/workspace/accelerator`.
