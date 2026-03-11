# CI/CD and Testing

This page describes how to run tests locally and what automated checks run in CI.

## Running tests locally

The following assumes the Python environment created in [Getting Started](./getting-started.md#setup-a-python-environment)
has been created and activated.

### Static checks (linting, formatting)

Static checks are enforced using [prek](https://pypi.org/project/prek/) (a pre-commit
runner). To run all checks:

```bash
prek run --all-files
```

To install the pre-commit hook so checks run automatically on each commit:

```bash
prek install
```

### elt-common unit tests

The `elt-common` package has unit tests under `elt-common/tests/`. To run them:

```bash
cd elt-common
uv pip install --editable . --group dev
pytest tests
```

### End-to-end warehouse tests

End-to-end tests verify that ingestion scripts work correctly against a real
local Iceberg catalog. They require the Docker Compose services to be running
(see [Getting Started](./getting-started.md)).

```bash
# Ensure services are up
cd infra/local
docker compose up --wait
# Run e2e tests
cd ../../warehouses
pytest
```

## CI workflows

GitHub actions is used as CI. See [../.github/workflows/](../.github/workflows/) for checks that run
through GitHub actions.
