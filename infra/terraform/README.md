# Cloud infrastructure provisioning

This directory contains [Terraform](https://developer.hashicorp.com/terraform) code to provision
resources on an Openstack cloud. *Note: Terraform no longer has an open-source license. [OpenTofu](https://opentofu.org/) is a drop-in replacement supported by CNCF, the `terraform` command can be replaced by `tofu` whereever it appears in external documentation.*

## Prerequisites

- Install OpenTofu using their documented method for your platform: <https://opentofu.org/docs/intro/install/>
- Run `tofu init` in this directory.
