# Deployment

_The system is currently deployed on the [SCD cloud](https://cloud.stfc.ac.uk/)._
_In theory it can be deployed on any cloud infrastructure but it will require_
_changes before this will work._

_It is not yet a production-grade, HA system._

There are several prerequisites steps required before deployment can begin. Please
read [them here](./prerequisites.md).

## Provision VMs

If this is the first run of terraform/tofu, initialize the terraform directory:

```bash
> cd infra/ansible/terraform
> tofu init
```

Choose the environment you are configuring for, `dev` or `qa`, and create a workspace:

```bash
> tofu workspace create <dev|qa>
> tofu workspace select <dev|qa>
```

Now provision the resources:

```bash
> tofu plan -var-file <tfvars-file> -var cloud_name=<name-in-clouds-yaml>
> tofu apply -var-file <tfvars-file> -var cloud_name=<name-in-clouds-yaml>
```

Move the newly generated inventory `.ini` file to `infra/ansible/inventories/<dev|qa>`.

### Interactive SSH access

The nodes are on a private network with the floating IP attached to the Traefik node.
`ssh` access to nodes other than Traefik requires a proxy command - the inventory is
configured for Ansible to understand this so no further configuration is required for
running Ansible. To be able to access the nodes via interactive ssh add the following
to `$HOME/.ssh/config`:

```text
Host jumphost-lakehouse-qa
   User ubuntu
   HostName 130.246.214.124

 Host 192.168.43.*
   ProxyJump jumphost-lakehouse
   StrictHostKeyChecking accept-new

 Host jumphost-lakehouse-dev
   User ubuntu
   HostName 130.246.212.128

 Host 192.168.44.*
   ProxyJump jumphost-lakehouse-dev
   StrictHostKeyChecking accept-new
```

## Services

Deploy the services using Ansible:

```bash
> cd infra/ansible
> ansible-playbook -i inventories/<dev|qa>/inventory.ini site.yml [-e lakekeeper_admin_user=<admin_email>]
```

The variable `lakekeeper_admin_user` is required the first time the playbooks are run. Further runs will add additional users
as Lakekeeper admins.

Once deployed the services are available at:

| Service | URL |
| --- | --- |
| Keycloak | <https://analytics.isis.cclrc.ac.uk/auth> |
| Lakekeeper UI | <https://analytics.isis.cclrc.ac.uk/iceberg/ui> |
| Superset (accelerator) | <https://analytics.isis.cclrc.ac.uk/workspace/accelerator> |
| Superset (experiment_ops) | <https://analytics.isis.cclrc.ac.uk/workspace/experiment_ops> |

## Certificates

See [certificates](certificates.md) for details on certificate management.
