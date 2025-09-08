# elt-common

A thin package around [`dlt`](https://dlthub.com) to provide functionality common
to each pipeline.

## Development setup

Development requires the following tools:

- [uv](https://docs.astral.uv/uv/): Used to manage both Python installations and dependencies
- [docker & docker compose]: Used for standing up services for end-to-end testing.

### Setting up a Python virtual environment

Once `uv` is installed, create an environment and install the `elt-common`
package in editable mode using the following command:

```bash
> uv pip install --editable . --group dev
```

## Running end-to-end tests

The end-to-end (e2e) tests for the `pyiceberg` destination require a running Iceberg
rest catalog to test complete functionality.
A (`docker-compose`)[./tests/docker-compose.yml] file is provided to both run the
required services and provide a `python-uv` service for executing test commands.
Please ensure you have docker and docker compose available on your command line
before continuing.

To run the end-to-end tests, from this directory execute

```bash
> docker compose -f tests/docker-compose.yml run python-uv uv run pytest tests/e2e_tests
```

When you have finished running the tests run

```bash
> docker compose -f tests/docker-compose.yml down
```

to bring down the dependent services.

### Debugging

Using a debugger to debug the end-to-end tests is more complicated as it requires
the dependent services to be accessible using their service names from within
the compose file.

To workaround this the `/etc/hosts` file can be edited to map the service names
to localhost (127.0.0.1). Open `/etc/hosts` and add

```text
# docker compose services
127.0.0.1 minio
127.0.0.1 keycloak
127.0.0.1 lakekeeper
```

Now bring up the services:

```bash
> docker compose -f tests/docker-compose.yml up -d
```

and start your debugger as normal.
