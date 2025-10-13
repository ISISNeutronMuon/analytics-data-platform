#!/bin/bash
set -ux

KC_ADM=/opt/keycloak/bin/kcadm.sh
KC_SERVER=http://keycloak:8080/auth

# authenticate
$KC_ADM config credentials \
  --server "$KC_SERVER" \
  --realm master \
  --user "$KC_BOOTSTRAP_ADMIN_USERNAME" \
  --password "$KC_BOOTSTRAP_ADMIN_PASSWORD"

####################
# realms
####################
$KC_ADM create realms \
  --set realm="$KC_REALM_NAME" \
  --set enabled=true

####################
# client scopes
####################
$KC_ADM create client-scopes \
  --target-realm "$KC_REALM_NAME" \
  --file - << EOF
{
  "name": "lakekeeper",
  "description": "Add aud=lakekeeper scope",
  "protocol": "openid-connect",
  "protocolMappers": [
    {
      "name": "Add Lakekeeper audience",
      "protocol": "openid-connect",
      "protocolMapper": "oidc-audience-mapper",
      "config": {
        "id.token.claim": "false",
        "introspection.token.claim": "true",
        "access.token.claim": "true",
        "included.custom.audience": "lakekeeper"
      }
    }
  ]
}
EOF

####################
# Clients
####################
$KC_ADM create clients \
  --target-realm "$KC_REALM_NAME" \
  --set clientId=lakekeeper \
  --set publicClient=true \
  --set 'redirectUris=["*"]' \
  --set 'optionalClientScopes=["lakekeeper", "address", "phone", "offline_access", "organization", "microprofile-jwt"]' \
  --set 'attributes={ "access.token.lifespan": 3600 }'

$KC_ADM create clients \
  --target-realm "$KC_REALM_NAME" \
  --set clientId=machine-infra \
  --set publicClient=false \
  --set serviceAccountsEnabled=true \
  --set 'redirectUris=["*"]' \
  --set 'optionalClientScopes=["lakekeeper", "address", "phone", "offline_access", "organization", "microprofile-jwt"]' \
  --set 'attributes={ "access.token.lifespan": 600 }' \
  --set 'secret=s3cr3t'

$KC_ADM create clients \
  --target-realm "$KC_REALM_NAME" \
  --set clientId=trino \
  --set publicClient=false \
  --set serviceAccountsEnabled=true \
  --set 'redirectUris=["*"]' \
  --set 'optionalClientScopes=["lakekeeper", "address", "phone", "offline_access", "organization", "microprofile-jwt"]' \
  --set 'attributes={ "access.token.lifespan": 600 }' \
  --set 'secret=s3cr3t'
