# elt-common

A thin package around [`dlt`](https://dlthub.com) to provide functionality common
to each pipeline.

## Development setup

Development requires the following tools:

- [uv](https://docs.astral.uv/uv/): Used to manage both Python installations and dependencies
- [docker & docker compose]: Used for standing up services for end-to-end testing.

### Setting up a Python virtual environment

Once `uv` is installed, create an environment and install the `elt-common`
package in editable mode, along with the development dependencies:

```bash
> uv venv
> source .venv/bin/activate
> uv pip install --editable . --group dev
```

## Running unit tests

Run the unit tests using `pytest`:

```bash
> pytest tests/unit_tests
```

## Running end-to-end tests

The end-to-end (e2e) tests for the `pyiceberg` destination require a running Iceberg
rest catalog to test complete functionality.
The local, docker-compose-based configuration provided by
[infra/local/docker-compose.yml](../infra/local/docker-compose.yml) is the easiest way to
spin up a set of services compatible with running the tests.
_Note the requirement to edit `/etc/hosts` described in [here](../infra/local/README.md)._

Once the compose services are running, execute the e2e tests using `pytest`:

```bash
> pytest tests/e2e_tests
```
