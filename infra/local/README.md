# Development infrastructure

The services defined here are intended for local development and should not be used for production.
We do not use https for local development, with the exception of Trino that requires it, due to
complications of ensuring self-signed certificates are trusted correctly across the host and
all service containers.

:exclamation: *Repeat: This configuration should not be used in production.*. :exclamation:

## Local set-up

The service can be set up locally using the instructions provided on [`getting-started.md`](/docs-devel/getting-started.md#setup-etchosts).

## Superset instances

There are two independent Superset stacks, `facility_ops` and `fase`. Shared service
definitions live in `docker-compose-superset.base.yml` and are pulled in by each
per-instance compose file via `extends:`. Each instance has its own
`superset_config.py` (under `superset/<instance>/pythonpath/`), env override file
(`env-superset-<instance>`) and dedicated metadata database (created automatically
by the instance's `superset-<instance>-db-bootstrap` service).

Bring up an instance alongside the core stack (the `superset` profile enables the
init/db-bootstrap services):

```sh
# facility_ops
docker compose -f docker-compose.yml -f docker-compose-superset-facility_ops.yml --profile superset up

# fase
docker compose -f docker-compose.yml -f docker-compose-superset-fase.yml --profile superset up
```

Add `--profile superset-worker` to also start the Celery worker and beat services.
