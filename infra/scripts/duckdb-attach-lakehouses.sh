#!/usr/bin/env bash
# Launches an interactive DuckDB CLI session attached to the Iceberg Lakehouse warehouses of
# the given environment, without persisting any credentials to disk (the DuckDB init script is
# passed via process substitution).
#
# For "local" the credentials are read from infra/local/env-local (the local docker-compose
# stack's fixed, non-secret development credentials).
#
# For "dev"/"qa" the credentials are fetched from Vault. Requires `curl` and `jq` on PATH, and a
# Vault token, see docs-devel/deployment/prerequisites.md#vault-access.
#
# Usage:
#   ./duckdb-attach.sh local
#   VAULT_TOKEN=$(cat ansible/.vault_token) ./duckdb-attach.sh <dev|qa>
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Lakekeeper warehouses, see infra/ansible/group_vars/all/all.yml#lakekeeper_catalog.
warehouse_names=(facility_ops_landing facility_ops)

usage() {
  echo "Usage: $0 <local|dev|qa>" >&2
  exit 1
}

[[ $# -eq 1 ]] || usage
deploy_env=$1

command -v duckdb >/dev/null 2>&1 || {
  echo "error: 'duckdb' is required but was not found on PATH" >&2
  exit 1
}

case "$deploy_env" in
local)
  # shellcheck disable=SC1091
  source "${script_dir}/local/env-local"

  s3_access_key_id="${MINIO_ROOT_USER}"
  s3_access_secret="${MINIO_ROOT_PASSWORD}"
  # MinIO API port, see the adp-router service in infra/local/docker-compose.yml.
  s3_endpoint="http://${ROUTER_HOSTNAME_EXTERNAL}:59000"
  keycloak_token_uri="${KEYCLOAK_REALM_EXTERNAL}/protocol/openid-connect/token"
  lakekeeper_catalog_uri="http://${ROUTER_HOSTNAME_EXTERNAL}:${ROUTER_PORT_HTTP}/iceberg/catalog"
  # Fixed local-only client credential, see infra/local/keycloak/bootstrap.sh. Not a real secret.
  oauth2_client_id="machine-infra"
  oauth2_client_secret="s3cr3t"
  ;;
dev | qa)
  for cmd in curl jq; do
    command -v "$cmd" >/dev/null 2>&1 || {
      echo "error: '${cmd}' is required but was not found on PATH" >&2
      exit 1
    }
  done

  : "${VAULT_TOKEN:?VAULT_TOKEN must be set, see docs-devel/deployment/prerequisites.md#vault-access}"
  vault_addr="${VAULT_ADDR:-https://secrets.isis.rl.ac.uk}"

  # Non-secret, per-environment configuration. Mirrors the equivalent values in
  # infra/ansible/inventories/<env>/group_vars/all/all.yml.
  case "$deploy_env" in
  dev)
    top_level_domain="dev-analytics.isis.cclrc.ac.uk"
    s3_endpoint="https://${top_level_domain}:9000"
    s3_secret_name="minio-creds"
    s3_access_key_field="user"
    s3_secret_key_field="password"
    ;;
  qa)
    top_level_domain="analytics.isis.cclrc.ac.uk"
    s3_endpoint="https://s3.echo.stfc.ac.uk"
    s3_secret_name="s3_echo"
    s3_access_key_field="access_key_id"
    s3_secret_key_field="access_secret"
    ;;
  esac

  # Constant across environments, see infra/ansible/group_vars/all/all.yml.
  keycloak_token_uri="https://${top_level_domain}/auth/realms/isis-analytics-data-platform/protocol/openid-connect/token"
  lakekeeper_catalog_uri="https://${top_level_domain}/iceberg/catalog"
  secrets_prefix="analytics-data-platform/data/${deploy_env}"

  # Reads a Vault KV v2 secret and prints its "data" object as JSON.
  vault_get() {
    local path=$1
    curl --fail --silent --show-error \
      --header "X-Vault-Token: ${VAULT_TOKEN}" \
      "${vault_addr}/v1/${path}" | jq -e '.data.data'
  }

  s3_secrets=$(vault_get "${secrets_prefix}/${s3_secret_name}")
  keycloak_client_secrets=$(vault_get "${secrets_prefix}/keycloak_client_secrets")

  s3_access_key_id=$(jq -er ".${s3_access_key_field}" <<<"${s3_secrets}")
  s3_access_secret=$(jq -er ".${s3_secret_key_field}" <<<"${s3_secrets}")
  oauth2_client_id="trino"
  oauth2_client_secret=$(jq -er '.trino' <<<"${keycloak_client_secrets}")
  ;;
*)
  usage
  ;;
esac

attach_statements=""
for warehouse_name in "${warehouse_names[@]}"; do
  attach_statements+=$(
    cat <<SQL

ATTACH '${warehouse_name}' AS ${warehouse_name} (
    TYPE iceberg,
    ENDPOINT '${lakekeeper_catalog_uri}',
    SECRET 'lakekeeper_secret'
);
SQL
  )
done

init_sql=$(
  cat <<SQL
SET s3_access_key_id = '${s3_access_key_id}';
SET s3_secret_access_key = '${s3_access_secret}';
SET s3_endpoint = '${s3_endpoint}';

CREATE SECRET lakekeeper_secret (
    TYPE iceberg,
    CLIENT_ID '${oauth2_client_id}',
    CLIENT_SECRET '${oauth2_client_secret}',
    OAUTH2_SCOPE 'lakekeeper',
    OAUTH2_SERVER_URI '${keycloak_token_uri}'
);

LOAD httpfs;
${attach_statements}
SQL
)

echo "Attaching to '${deploy_env}' Lakehouse warehouses: ${warehouse_names[*]}" >&2
exec duckdb -init <(printf '%s\n' "${init_sql}")
