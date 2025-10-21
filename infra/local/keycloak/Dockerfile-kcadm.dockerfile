# Small wrapper image to add jq to the build for bootstrapping Keycloak

ARG base
FROM ${base}

ADD --chmod=755 https://github.com/jqlang/jq/releases/download/jq-1.8.1/jq-linux-amd64 /usr/local/bin/jq
