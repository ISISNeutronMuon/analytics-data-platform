#!/bin/bash
# Runs bootstrapping process for the stack:
#
#   - Create required warehouses in Lakekeeper
#   - Create required catalogs in Trino
set -euo pipefail

BOOTSTRAP_SCRIPTS_DIR=/opt/work
WAREHOUSE_PREFIXES="facility_ops fase"

function bootstrap-lakekeeper-warehouse() {
    local name=$1
    # Generate the Lakekeeper warehouse config to a temporary file.
    uv run $BOOTSTRAP_SCRIPTS_DIR/generate-warehouse-json.py "$name" "$ADMIN_USER" "$ADMIN_PASSWORD" > "/tmp/$name.json"
    uv run $BOOTSTRAP_SCRIPTS_DIR/bootstrap-warehouse.py \
        --lakekeeper-project-name "$KC_REALM_NAME" \
        --keycloak-url "$KEYCLOAK_URL_INTERNAL" \
        --keycloak-admin-credentials "$KC_BOOTSTRAP_ADMIN_USERNAME:$KC_BOOTSTRAP_ADMIN_PASSWORD" \
        --keycloak-user-realm "$KC_REALM_NAME" \
        --bootstrap-credentials "machine-infra:s3cr3t" \
        --token-scope lakekeeper \
        --server-admin "$ADMIN_USER" \
        --log-level=DEBUG \
        --warehouse-json-file "/tmp/$name.json" \
        "http://lakekeeper:8181"
}

function bootstrap-trino-catalog() {
    local name=$1
    uv run $BOOTSTRAP_SCRIPTS_DIR/create-trino-catalog.py "$name"
}

for prefix in ${WAREHOUSE_PREFIXES}; do
    for name in "${prefix}_landing" "${prefix}"; do
        bootstrap-lakekeeper-warehouse "$name"
        bootstrap-trino-catalog "$name"
    done
done
