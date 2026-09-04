#!/bin/bash
# Uses psql to create a database with a given name
set -euo pipefail

if [ $# -ne 3 ]; then
  echo "Usage: $0 host user dbname"
  echo
  echo "Create a new DB with the specified name on the given host."
  exit 1
fi


DB_HOST=$1
DB_USER=$2
DB_NAME=$3

echo "Ensuring database $DB_NAME exists"
psql -v ON_ERROR_STOP=1 --host="$DB_HOST" --username="$DB_USER" <<-EOSQL
    SELECT 'CREATE DATABASE $DB_NAME' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$DB_NAME')\\gexec
EOSQL
