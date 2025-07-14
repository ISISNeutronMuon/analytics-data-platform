# Ansible Scripts

The Ansible configuration here is used to deploy the Lakehouse platform. It has
currently only been tested using the STFC cloud but in theory it will work with
other cloud providers or Linux VMs.

## Setup Ansible

It is recommended to use a virtual environment to set up Ansible but you can use
any Python installation that has `pip` installed. The version of Python required
is recorded in [./python-version](./python-version).

[uv](https://docs.astral.sh/uv/pip/environments/) is a popular and fast tool
for managing Python environments and is the recommended tool to use.

Create a virtual environment

```sh
uv venv
```

Install the requirements using `requirements-python.txt`

```sh
uv pip install -r ./requirements-python.txt
```

Install the additional Ansible Galaxy roles with:

```sh
uv run ansible-galaxy role install --roles-path galaxy_roles -r requirements-ansible-galaxy.yml
```

The `galaxy_roles` path has been added to `.gitignore`.

## Openstack preparation

TODO: Tidy up

- Two floating IPs are required (defined in [./group_vars/all/openstack.yml](./group_vars/all/openstack.yml)):
  - One for the management/SSH jump node. Check `openstack_management_vm_fip` value.
  - One for the Traefik load balancer node. Check the `openstack_reverse_proxy_fip` value.

## Creating the stack

TODO: Tidy up

In the following set of instructions the following variables are required:

- `[CLOUDS_YAML_NAME]`: the name of the target Openstack cloud within the `clouds` block of the `~/.config/clouds.yaml`.
- `[SSH_KEY_NAME]`: the name of the SSH key already stored in [Openstack](https://openstack.stfc.ac.uk/project/key_pairs)
  that will be added to newly created servers.

### Create management VM

_If a previous VM was deleted then ensure you remove the host keys for the IP address from `~/.ssh_known_hosts` to avoid connection issues._:

```sh
sed -i ""  -E -e '/^INSERT MANAGEMENT FLOATING IP HERE/d'  ~/.ssh/known_hosts`
```

Create the management VM first:

```sh
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i localhost -e openstack_cloud_name=[CLOUDS_YAML_NAME] -e openstack_key_name=[SSH_KEY_NAME] playbooks/management/create.yml`
```

This is assigned a floating IP and acts as an SSH proxy into the private network that the service nodes live on.
Without this SSH connections to the service nodes would not be possible.

Create the service node VMs:

```sh
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i localhost -e openstack_cloud_name=[CLOUDS_YAML_NAME] -e openstack_key_name=[SSH_KEY_NAME] -e inventory_filename=$PWD/inventory-virtnet.ini playbooks/cloud/vms_create.yml`
```

### Deploy services

- Remove any old VM host keys from SSH known_hosts: `sed -i ""  -E -e '/^192/d'  ~/.ssh/known_hosts` (this will remove all 192 IPs )
- Run `ANSIBLE_STDOUT_CALLBACK=ansible.posix.debug ansible-playbook -i inventory-virtnet.ini playbooks/cloud/services_deploy.yml`
- You'll need to accept each key as it runs through the setup or run with `ANSIBLE_HOST_KEY_CHECKING=False` prefixed to the command.
