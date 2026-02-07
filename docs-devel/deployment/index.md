# Deployment

_The system is currently deployed on the [SCD cloud](https://cloud.stfc.ac.uk/)._
_In theory it can be deployed on any cloud infrastructure but it will require_
_changes before this will work._

_It is not yet a production-grade, HA system._

There are several prerequisites steps required before deployment can begin. Please
read [them here](./prerequisites.md).

## Provision VMs

Choose the environment you are configuring for, `dev` or `qa`, and provision the cloud resources:

```bash
> cd infra/ansible/terraform
> tofu init
> tofu plan -var-file <tfvars-file> -var cloud_name=<name-in-clouds-yaml>
> tofu apply -var-file <tfvars-file> -var cloud_name=<name-in-clouds-yaml>
```

Move the newly generated inventory `.ini` file to `infra/ansible/inventories/<dev|qa>`.

## Services

Deploy the services using Ansible:

```bash
> cd infra/ansible
> ansible-playbook -i inventories/<dev|qa>/inventory.ini site.yml -e lakekeeper_admin_user=<admin_email>
```

The `-e lakekeeper_admin_user=<admin_email>` argument is only required the first time Lakekeeper is deployed.

Once deployed the services are available at:

- Keycloak: <https://\domain\>/iceberg>
- Lakekeeper: <https://\<domain\>/authn>
- Superset instances:
   - <https://\<domain\>/workspace/accelerator>
