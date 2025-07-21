# Development infrastructure

The services defined here are intended for local development and should not be used for production.
The base url for all services is configured to be *https://analytics.local*.

## Certificates

Install [mkcert](https://github.com/FiloSottile/mkcert?tab=readme-ov-file#installation) and
generate a certificate for local development:

```sh
cd certs
mkcert analytics.local
cat analytics.local.pem analytics.local-key.pem > analytics.local-combined.pem
```

The configuration defined below will expect the generated files to be found in the `certs` subdirectory.

## /etc/hosts

Add the following lines to `/etc/hosts`:

```sh
127.0.0.1  analytics.local  # Local DNS name
127.0.0.1  minio  # Required by clients on the host access MinIO directly
```

The base url for the local services is https://analytics.local:8443. The `minio` entry is required
for `pyiceberg` clients running on the host to find the MinIO object store.

### Python

Most Python http libraries don't use the system CA bundle and instead use a separate certificate
bundle provided by the `certifi` package.

For Python `requests` to trust the `mkcert` certificates without altering the certificate bundle
set the environment variable:

```sh
REQUESTS_CA_BUNDLE=`mkcert -CAROOT`/rootCA.pem
```

To permanently trust the `mkcert` root CA, add it to the `certifi` bundle in the Python environment

```sh
cat "`mkcert -CAROOT`/rootCA.pem" >> `python -m certifi`
```

**Only do this for virtual environments that can easily be deleted and recreated.**

## dlt secrets

Create and add the following to `$HOME/.dlt/secrets.toml`:

```sh
[destination.pyiceberg]
bucket_url = "s3://local-lakehouse-isis"

[destination.pyiceberg.credentials]
uri = "https://analytics.local:8443/lakekeeper/catalog"
warehouse = "isis"
```

**Do not put production secrets here.**

## Services

Bring up the services with `docker compose`:

```sh
docker compose up -d
```
