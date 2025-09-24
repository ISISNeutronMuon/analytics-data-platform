# Development infrastructure

The services defined here are intended for local development and should not be used for production.
We do not use https for local development due to complications of ensuring self-signed
certificates are trusted correctly across the host and all service containers.
*Repeat: This configuration should not be used in production.*.

The base url for all services is configured to be *http://localhost:58080*.
Port 58080 ensures we don't clash with anything running on the host.

## dlt secrets

Create and add the following to `$HOME/.dlt/secrets.toml`:

```sh
[destination.pyiceberg]
bucket_url = "s3://playground"

[destination.pyiceberg.credentials]
uri = "http://localhost:58080/internal/iceberg/catalog"
warehouse = "playground"
```

**Do not put production secrets here.**

## Services

Bring up the services with `docker compose`:

```sh
docker compose up -d
```
