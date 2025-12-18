# Development infrastructure

The services defined here are intended for local development and should not be used for production.
We do not use https for local development, with the exception of Trino that requires it, due to
complications of ensuring self-signed certificates are trusted correctly across the host and
all service containers.
*Repeat: This configuration should not be used in production.*.

## Set Up

An additional entry in `/etc/hosts` on the host machine is required:

```sh
127.0.0.1    adp-router
```

Some tools, such as PyIceberg and Superset in the browser, need to access services both outside
and inside the container network. The `adp-router` service is a Traefik instance that routes
traffic to match the url layout of the production deployment. By defining the above `/etc/hosts`
entry a request will hit the router regardless of whether it is inside or outside of the container
network.

To test ELT scripts based on the Python package `dlt`, create and add the following to
`$HOME/.dlt/secrets.toml`:

```toml
[destination.pyiceberg]
bucket_url = "s3://playground"
[destination.pyiceberg.credentials]
uri = "http://localhost:50080/iceberg/catalog"
warehouse = "playground"
oauth2_server_uri = "http://localhost:50080/auth/realms/analytics-data-platform/protocol/openid-connect/token"
client_id = "machine-infra"
client_secret = "s3cr3t"
scope = "lakekeeper"
```

## Start services

Bring up the services with `docker compose`:

```sh
docker compose up -d
```

Services:

- Keycloak identity provider: http://localhost:50080/auth
  - `master` realm credentials: _admin/admin_
  - `analytics-data-platform` realm credentials: _adpsuperuser/adppassword_
- Lakekeeper iceberg catalog UI: http://localhost:50080/iceberg/ui.
  - Use the same credentials as the `analytics-data-platform` realm
- Superset BI tool: http://localhost:50080/workspace/playground.
  - Use the same credentials as the `analytics-data-platform` realm
- Trino endpoint: https://localhost:58443 (note the https as this is required by Trino. You may need to use an "--insecure" flag)
