# Development infrastructure

The services defined here are intended for local development and should not be used for production.
We do not use https for local development due to complications of ensuring self-signed
certificates are trusted correctly across the host and all service containers.
*Repeat: This configuration should not be used in production.*.

## Services

Bring up the services with `docker compose`:

```sh
docker compose up -d
```

See header comments in [docker-compose.yml](./docker-compose.yml) for details of routes to the
individual services.

## Running scripts on the host, including Python e2e tests.

See header comments in [docker-compose.yml](./docker-compose.yml) for extra configuration of
`/etc/hosts` required to run scripts on the host that access the Lakekeeper catalog.
