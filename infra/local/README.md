# Development infrastructure

The services defined here are intended for local development and should not be used for production.

## Hostname

Local development occurs under the hostname `analytics.dev`. Add `127.0.0.1  analytics.dev` to your
`/etc/hosts` file.

## Certificates

Local development certificates are generated using the [mkcert](https://github.com/FiloSottile/mkcert)
utility. Once the [installation](https://github.com/FiloSottile/mkcert?tab=readme-ov-file#installation)
steps have been followed generate a certificate for local development:

```sh
cd certs
mkcert analytics.dev
cat analytics.dev.pem analytics.dev-key.pem > analytics.dev-combined.pem
```

The configuration defined below will expect the generated files to be found in the `certs` subdirectory.

## Services

The development services run through `docker compose`. To avoid duplicating configurations across
the deployment and local development an Ansible playbook is used to generate the development
configuration.

Generate the `docker-compose.yaml` for local development:

```sh
```
