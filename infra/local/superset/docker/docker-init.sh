#!/usr/bin/env bash
set -e

#
# Always install local overrides first
#
/app/docker/docker-bootstrap.sh

echo_step() {
cat <<EOF

######################################################################


Init [${1}] -- ${2}


######################################################################

EOF
}

# Initialize the database
echo_step "Starting" "Applying DB migrations"
superset db upgrade
echo_step "Complete" "Applying DB migrations"

# Create an admin user (if supplied)
if [ -n "$SUPERSET_ADMIN_USER" ]; then
  echo_step "Starting" "Setting up admin user"
  superset fab create-admin \
                --username "$SUPERSET_ADMIN_USER" \
                --firstname "$SUPERSET_ADMIN_FIRSTNAME" \
                --lastname "$SUPERSET_ADMIN_LASTNAME" \
                --email "$SUPERSET_ADMIN_EMAIL" \
                --password "$SUPERSET_ADMIN_PASSWORD"
  echo_step "Complete" "Setting up admin user"
fi

# Create default roles and permissions
echo_step "Starting" "Setting up roles and perms"
superset init
echo_step "Complete" "Setting up roles and perms"

# Create a database connection
echo_step "Starting" "Setting up Iceberg catalog connection"
superset set-database-uri \
  --database_name "$WAREHOUSE_NAME" \
  --uri "trino://$TRINO_USER:$TRINO_PASSWORD@$ROUTER_HOSTNAME_INTERNAL:$TRINO_HTTPS_PORT/$WAREHOUSE_NAME?verify=false"
echo_step "Complete" "Setting up Iceberg catalog connection"
