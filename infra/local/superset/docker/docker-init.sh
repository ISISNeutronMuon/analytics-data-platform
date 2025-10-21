#!/usr/bin/env bash
set -e

#
# Always install local overrides first
#
/app/docker/docker-bootstrap.sh

STEP_CNT=2

echo_step() {
cat <<EOF

######################################################################


Init Step ${1}/${STEP_CNT} [${2}] -- ${3}


######################################################################

EOF
}

# Initialize the database
echo_step "1" "Starting" "Applying DB migrations"
superset db upgrade
echo_step "1" "Complete" "Applying DB migrations"

# Create an admin user
# echo_step "2" "Starting" "Setting up admin user admin "
# superset fab create-admin \
#               --username "$SUPERSET_ADMIN_USER" \
#               --firstname "$SUPERSET_ADMIN_FIRSTNAME" \
#               --lastname "$SUPERSET_ADMIN_LASTNAME" \
#               --email "$SUPERSET_ADMIN_EMAIL" \
#               --password "$SUPERSET_ADMIN_PASSWORD"
# echo_step "2" "Complete" "Setting up admin user"

# Create default roles and permissions
echo_step "2" "Starting" "Setting up roles and perms"
superset init
echo_step "2" "Complete" "Setting up roles and perms"

# Create a database connection
# echo_step "4" "Starting" "Setting up Iceberg catalog connection"
# superset set-database-uri \
#   --database_name "$SUPERSET_DB_SCHEMA_NAME" \
#   --uri "trino://superset:adppassword@trino:8080/$SUPERSET_DB_SCHEMA_NAME"
# echo_step "4" "Complete" "Setting up data source database connection"
