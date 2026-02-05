# Prerequisites

The following resources are required before deployment can proceed:

- [OpenTofu](#terraformopentofu)
- [Python environment configured for running Ansible](#python-environment)
- [Ansible vault password](#ansible-vault)
- [Openstack clouds.yaml](#openstack-api--vm-credentials)
- [A shared filesystem through a Manila share](#manila-share)
- [Object storage](#object-store)
- [Networking](#networking)

## Terraform/OpenTofu

[Terraform](https://developer.hashicorp.com/terraform) is used to provision resources on an Openstack
cloud. See the resources definitions in [terraform](../../infra/ansible/terraform/).

*Note: Terraform no longer has an open-source license. [OpenTofu](https://opentofu.org/) is a*
*drop-in replacement supported by CNCF, the `terraform` command can be replaced by `tofu`*
*wherever it appears in external documentation.*

- Install OpenTofu using their documented method for your platform: <https://opentofu.org/docs/intro/install/>
- Run `tofu init` in the `../../infra/ansible/terraform/` directory.

## Python environment

Ansible requires a Python environment. These instructions assume the use of the
[uv](https://docs.astral.sh) tool for managing both Python and the virtual
environments. See [uv installation](https://docs.astral.sh/uv/getting-started/installation/)
& [installing Python](https://docs.astral.sh/uv/guides/install-python/) guides.

- Create a virtual environment in the `<repo_root>/ansible` directory

```sh
cd <repo_root>/ansible
uv venv
```

- Install the Python requirements & Ansible Galaxy requirements

```sh
uv pip install -r requirements-python.txt
uv run ansible-galaxy role install --roles-path galaxy_roles -r requirements-ansible-galaxy.yml
```

## Openstack API & VM credentials

Interacting with the Openstack API requires access credentials configured
in a `~/.config/openstack/clouds.yaml` file.
See the [SCD cloud documentation](https://stfc.atlassian.net/wiki/spaces/CLOUDKB/pages/211583200/Python+SDK#Setting-Up-Clouds.yaml) for details on how to configure this file.

An ssh key is required in order to access the VMs.
See the [SCD cloud documentation](https://stfc.atlassian.net/wiki/spaces/CLOUDKB/pages/211910700/Adding+Your+SSH+Key+into+the+OpenStack+Web+Interface) for details on how to configure your SSH key.

## Ansible vault

The project secrets are stored using [Ansible Vault](https://docs.ansible.com/ansible/latest/vault_guide/index.html). The password is required and can be supplied either on the command line with each ansible command or
stored locally in a `<repo_root>/ansible/.vault_pass` file. **Do not share this password or commit the .vault_pass file to Git.**

## Manila share

*Used for: Persistent storage for running system services, e.g. database data. Not used for user data.*

A Manila/CephFS share of at least 5TB is required. Once a quota has been assigned to the project:

- Create a new share, under *Project->Share->Shares*, and mark it private.
- Click on the share, make note of the *Export locations.Path* value.
- Edit the `vault_cephfs_export_path` variable to match the value from the previous step.
- On the main *Shares* page click the down arrow on the side of the *EDIT SHARE*
  button and go to *Manage Rules*.
- Add a new rule and once created make note of the _Access Key` value.
- Edit the `vault_cephfs_access_secret` variable to match the value from the previous step.

## Object store

*Used for: Persistent storage of user data.*

This is currently expected to be configured to use the Echo object store.
The S3 endpoint is configured through the `s3_endpoint` ansible variable
in [infra/ansible](inventories/qa/group_vars/all/all.yml).

An access key and secret are configured in the vault. They cannot be managed through
the Openstack web interface, instead new keys and secrets are created using the
`openstack ec2 credentials` command.

In the [infra/ansible](../../infra/ansible) directory run `uv run openstack --os-cloud=<cloud_name> ec2 credentials create`
to create a new access key/secret pair. Update the Ansible vault accordingly.

## Networking

Requirements:

- floating IP:
   - Using the web interface create one from *Project->Network->Floating IPs*,
    using *ALLOCATE IP TO PROJECT*, ensuring a description is provided.
   - Place the value in the Terraform [environments tf vars file](../../infra/ansible/terraform/environments).
- DNS record pointing at the above floating IP
   - Place the value in [inventories/dev/group_vars/all/all.yml](../../infra/ansible/inventories/dev/group_vars/all/all.yml).
