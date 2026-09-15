#!/bin/bash
# Runs bootstrapping process for the stack:
#
#   - Create required warehouses in Lakekeeper
#   - Create required catalogs in Trino
set -euo pipefail

BOOTSTRAP_SCRIPTS_DIR=/opt/work
LAKEKEEPER_JSON_DIR=/opt/data/lakekeeper
TRINO_CATALOG_DIR=/opt/data/trino/catalog
WAREHOUSE_PREFIXES="facility_ops fase"

function bootstrap-lakekeeper-warehouse() {
    local name=$1
    local json_file="/tmp/$name.json"
    local warehouse_marker="$LAKEKEEPER_JSON_DIR/$name.json"

    if [[ -f "$warehouse_marker" ]]; then
        echo "Lakekeeper warehouse '$name' JSON already exists ($warehouse_marker). Skipping warehouse creation."
        return 0
    fi

    uv run $BOOTSTRAP_SCRIPTS_DIR/generate-warehouse-json.py "$name" > "$json_file"
    uv run $BOOTSTRAP_SCRIPTS_DIR/bootstrap-warehouse.py \
        --lakekeeper-project-name "$KC_REALM_NAME" \
        --keycloak-url "$KEYCLOAK_URL_INTERNAL" \
        --keycloak-admin-credentials "$KC_BOOTSTRAP_ADMIN_USERNAME:$KC_BOOTSTRAP_ADMIN_PASSWORD" \
        --keycloak-user-realm "$KC_REALM_NAME" \
        --bootstrap-credentials "$LOCAL_ADMIN_MACHINE:$LOCAL_PASSWORD" \
        --token-scope lakekeeper \
        --server-admin "$LOCAL_ADMIN_USER" \
        --log-level=DEBUG \
        --warehouse-json-file "$json_file" \
        "http://lakekeeper:8181"

    # Mark as done
    mv "$json_file" "$warehouse_marker"
}

function bootstrap-trino-catalog() {
    local name=$1
    local props_file="$TRINO_CATALOG_DIR/$name.properties"

    if [[ -f "$props_file" ]]; then
        echo "Trino catalog '$name' already exists ($props_file). Skipping catalog creation."
        return 0
    fi

    uv run $BOOTSTRAP_SCRIPTS_DIR/create-trino-catalog.py "$name"
}

# -----------------------------------------------------------------------------
# Create data directories and set permissions
# -----------------------------------------------------------------------------
mkdir -p $LAKEKEEPER_JSON_DIR
mkdir -p $TRINO_CATALOG_DIR
chown -R 1000:1000 $TRINO_CATALOG_DIR

# -----------------------------------------------------------------------------
# Bootstrap
# -----------------------------------------------------------------------------
for prefix in ${WAREHOUSE_PREFIXES}; do
    for name in "${prefix}_landing" "${prefix}"; do
        bootstrap-lakekeeper-warehouse "$name"
        bootstrap-trino-catalog "$name"
    done
done
