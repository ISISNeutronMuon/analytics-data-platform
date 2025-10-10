#!/bin/bash
set -u

KC_ADM=/opt/keycloak/bin/kcadm.sh
KC_SERVER=http://keycloak:8080/auth


echo "Creating realms and clients"
$KC_ADM config credentials \
  --server $KC_SERVER \
  --realm master \
  --user $KC_BOOTSTRAP_ADMIN_USERNAME \
  --password $KC_BOOTSTRAP_ADMIN_PASSWORD

# realm
$KC_ADM create realms \
  --set realm=$KC_REALM_NAME \
  --set enabled=true \
  --output

# client scopes
read -d '' lakekeeper_aud_mapper_json << EOF
[{
  "name": "Add lakekeeper audience",
  "protocol": "openid-connect",
  "protocolMapper": "oidc-audience-mapper",
  "consentRequired": false,
  "config": {
    "id.token.claim": "false",
    "lightweight.claim": "false",
    "access.token.claim": "true",
    "introspection.token.claim": "true",
    "included.custom.audience": "lakekeeper"
  }
}]
EOF

$KC_ADM create client-scopes \
  --target-realm $KC_REALM_NAME \
  --set name=lakekeeper \
  --set protocol=openid-connect \
  --set description="Lakekeeper scope" \
  --set protocolMappers="$lakekeeper_aud_mapper_json"

# clients
# $KC_ADM create clients \
#   --target-realm $KC_REALM_NAME \
#   --set clientId=lakekeeper \
#   --set secret=s3cr3t \
#   --
