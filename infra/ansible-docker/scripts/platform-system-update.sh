#!/bin/bash
# Apply system updates in a specified order to the platform nodes
INVENTORY=inventory.yml

function fail_with_message() {
  echo "$@"
  exit 1
}

function system_update() {
  local selection=$1
  ansible-playbook -i $INVENTORY -l "$selection" playbooks/system/system_update.yml
}

test -f $INVENTORY || fail_with_message "$INVENTORY must exist in current directory ($PWD)"

# Do updates in a top-down order
system_update elt
system_update superset_farm
system_update trino
system_update lakekeeper
system_update keycloak
system_update traefik
