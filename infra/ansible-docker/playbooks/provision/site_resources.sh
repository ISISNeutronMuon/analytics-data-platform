#!/bin/bash
# Thin-wrapper to provision all site resources at once.
#
# A "parent" playbook that imports child playbooks suffers from "variable leakage" as variables in
# the child "import_playbook" remain inside the parent run. This simply loops over each resource
# and runs Ansible separately. All cli args are passed to ansible-playbook.

# To run a single play place "site_resources.sh" with the usual "ansible-playbook" command.
PLAYBOOKS_DIR=playbooks/provision

if [ ! -d "$PLAYBOOKS_DIR" ]; then
  echo "$PLAYBOOKS_DIR" does not exist. Run this command from the root of the ansible directory.
  exit 1
fi

# The order of the resources is important - don't use a glob
for resource in network traefik keycloak lakekeeper; do
  ansible-playbook "$@" $PLAYBOOKS_DIR/$resource.yml || exit 1
done
