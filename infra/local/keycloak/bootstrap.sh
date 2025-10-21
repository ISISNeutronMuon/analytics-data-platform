#!/bin/bash
# See https://docs.redhat.com/en/documentation/red_hat_build_of_keycloak/26.2/html/server_administration_guide/admin_cli
set -ux

KC_ADM=/opt/keycloak/bin/kcadm.sh

function get_resource_id() {
  local endpoint=$1
  local fields=$2
  local jq_select=$3

  $KC_ADM get "$endpoint" \
    --fields "$fields" \
    | jq --raw-output ".[] | select($jq_select) | .id"
}

function client_scope_with_aud_mapper() {
  local scope_name=$1
  local audience=$2
  cat -<< EOF
{
  "name": "$scope_name",
  "protocol": "openid-connect",
  "protocolMappers": [
    {
      "name": "$scope_name audience mapper",
      "protocol": "openid-connect",
      "protocolMapper": "oidc-audience-mapper",
      "config": {
        "id.token.claim": "false",
        "introspection.token.claim": "true",
        "access.token.claim": "true",
        "included.custom.audience": "$audience"
      }
    }
  ]
}
EOF
}

# Expected environment variables
admin_user=$KC_BOOTSTRAP_ADMIN_USERNAME
admin_pass=$KC_BOOTSTRAP_ADMIN_PASSWORD
target_realm=$KC_REALM_NAME

# Args
kc_server=$1

# authenticate
$KC_ADM config credentials \
  --server "$kc_server" \
  --realm master \
  --user "$admin_user" \
  --password "$admin_pass"


####################
# realms
####################
realm_id=$(get_resource_id realms "id,realm" ".realm==\"$target_realm\"")
if [ -n "$realm_id" ]; then
  echo Realm "$target_realm" already exists. Skipping bootstrap.
  exit 0
fi

$KC_ADM create realms \
  --set realm="$target_realm" \
  --set enabled=true

####################
# client scopes
####################
$KC_ADM create client-scopes \
  --target-realm "$target_realm" \
  --body "$(client_scope_with_aud_mapper lakekeeper lakekeeper)"
$KC_ADM create client-scopes \
  --target-realm "$target_realm" \
  --body "$(client_scope_with_aud_mapper trino trino)"

# # by default the 'roles' claim is not included in userinfo but we need it for Superset to see the roles
id_scope_roles=$(get_resource_id realms/"$target_realm"/client-scopes "id,name" '.name=="roles"')
id_scope_mappper_roles=$(get_resource_id \
  realms/"$target_realm"/client-scopes/"$id_scope_roles"/protocol-mappers/models \
  "id,protocolMapper" \
  '.protocolMapper=="oidc-usermodel-realm-role-mapper"')
$KC_ADM update realms/"$target_realm"/client-scopes/"$id_scope_roles"/protocol-mappers/models/"$id_scope_mappper_roles" \
   --merge \
   --set 'config."userinfo.token.claim"="true"'

####################
# Clients
# If optionalClientScopes are provided then defaultClientScopes must be or they are all deleted
####################
# Confidential clients
$KC_ADM create clients \
  --target-realm "$target_realm" \
  --set clientId=machine-infra \
  --set publicClient=false \
  --set standardFlowEnabled=false \
  --set serviceAccountsEnabled=true \
  --set 'redirectUris=["*"]' \
  --set 'defaultClientScopes=["web-origins", "acr", "profile", "roles", "basic", "email"]' \
  --set 'optionalClientScopes=["lakekeeper", "address", "phone", "offline_access", "organization", "microprofile-jwt"]' \
  --set 'attributes={ "access.token.lifespan": 600 }' \
  --set 'secret=s3cr3t'
# Allow this account to administer the realm
$KC_ADM add-roles \
  --target-realm "$target_realm" \
  --uusername service-account-machine-infra \
  --cclientid realm-management \
  --rolename realm-admin

$KC_ADM create clients \
  --target-realm "$target_realm" \
  --set clientId=opa \
  --set publicClient=false \
  --set standardFlowEnabled=false \
  --set serviceAccountsEnabled=true \
  --set 'redirectUris=["*"]' \
  --set 'defaultClientScopes=["web-origins", "acr", "profile", "roles", "basic", "email"]' \
  --set 'optionalClientScopes=["lakekeeper", "address", "phone", "offline_access", "organization", "microprofile-jwt"]' \
  --set 'attributes={ "access.token.lifespan": 3600 }' \
  --set 'secret=s3cr3t'

$KC_ADM create clients \
  --target-realm "$target_realm" \
  --set clientId=trino \
  --set publicClient=false \
  --set standardFlowEnabled=true \
  --set directAccessGrantsEnabled=true \
  --set serviceAccountsEnabled=true \
  --set 'redirectUris=["*"]' \
  --set 'defaultClientScopes=["web-origins", "acr", "profile", "roles", "basic", "email"]' \
  --set 'optionalClientScopes=["trino", "lakekeeper", "address", "phone", "offline_access", "organization", "microprofile-jwt"]' \
  --set 'attributes={ "access.token.lifespan": 3600 }' \
  --set 'secret=s3cr3t'

# Public clients
$KC_ADM create clients \
  --target-realm "$target_realm" \
  --set clientId=lakekeeper \
  --set publicClient=true \
  --set 'redirectUris=["*"]' \
  --set 'defaultClientScopes=["web-origins", "acr", "profile", "roles", "basic", "email"]' \
  --set 'optionalClientScopes=["lakekeeper", "address", "phone", "offline_access", "organization", "microprofile-jwt"]' \
  --set 'attributes={ "access.token.lifespan": 3600 }'

$KC_ADM create clients \
  --target-realm "$target_realm" \
  --set clientId=superset \
  --set publicClient=true \
  --set 'redirectUris=["*"]' \
  --set 'attributes={ "access.token.lifespan": 3600 }'


####################
# Users
####################
uid_adp_superuser=$($KC_ADM create users \
    --target-realm "$target_realm" \
    --set username="$ADP_SUPERUSER" \
    --set firstName=Super \
    --set lastName=User \
    --set email=adpsuperuser@dev.com \
    --set enabled=true \
    --id)
$KC_ADM set-password \
  --target-realm "$target_realm" \
  --username "$ADP_SUPERUSER" \
  --new-password "$ADP_SUPERUSER_PASS"

####################
# Roles/Groups
####################
admin_role=adp_platform_admins
gid_admins=$(\
  $KC_ADM create groups \
    --target-realm "$target_realm" \
    --set name="$admin_role" \
    --id)
$KC_ADM update \
  --target-realm "$target_realm" \
  users/"$uid_adp_superuser"/groups/"$gid_admins"

$KC_ADM create roles \
    --target-realm "$target_realm" \
    --set name="$admin_role" \
    --id
$KC_ADM add-roles \
  --target-realm "$target_realm" \
  --gname "$admin_role" \
  --rolename "$admin_role"
