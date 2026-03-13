#!/bin/bash
# shellcheck disable=1091
# Execute scripts through Trino, connecting to the catalogs defined in the docker-compose.yml
# configuration. Authentication is enabled so connections require ssl and simply running
# 'docker compose exec trino' won't work. Instead connections go through Traefik that terminates
# the TLS connection.

if [ $# -ne 2 ]; then
  echo "Usage: $0 <catalog_name> <args_for_execute>"
  exit 1
fi

# single catalog argument
catalog=$1
shift

# local environment
source "$(dirname "$0")/env-local"

docker run \
    --rm \
    --interactive \
    --tty \
    --network host \
    --env TRINO_PASSWORD="$TRINO_PASSWORD" \
    trinodb/trino:477 \
    trino \
        --user "$TRINO_USER" \
        --password \
        --server https://adp-router:58443 \
        --insecure \
        --catalog "$catalog" \
        --execute "$*"
