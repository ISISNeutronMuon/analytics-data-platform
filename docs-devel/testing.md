# CI/CD and Testing

This page describes how to run tests locally and what automated checks run in CI.

## Running tests locally

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
uv venv && source .venv/bin/activate
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
cd warehouses
pytest
```

## CI workflows

The following GitHub Actions workflows run automatically on pull requests and pushes to `main`:

### Static checks (`ci-static.yml`)

- **Triggers**: All PRs and pushes to `main`.
- **What it does**: Installs `prek` and runs all static analysis checks across
  the entire repository.

### elt-common tests (`elt-common_tests.yml`)

- **Triggers**: PRs/pushes that modify files under `elt-common/` or the workflow itself.
- **What it does**: Installs the `elt-common` package with `uv sync --locked`, then
  runs `pytest` against the unit test suite.

### Warehouse end-to-end tests (`warehouses_e2e_tests.yml`)

- **Triggers**: PRs/pushes that modify `elt-common/`, warehouse extract/load code,
  the Lakekeeper bootstrap script, or the workflow itself.
- **What it does**: Brings up the full Docker Compose stack in CI, adds the
  `adp-router` host entry, installs dependencies, and runs `pytest` against
  the warehouse test suite with a live REST catalog.

### Superset Docker image (`docker-superset.yml`)

- **Triggers**: Changes to the Superset container image definition.
- **What it does**: Builds the custom Superset Docker image to verify it compiles
  successfully.

## Writing tests

- **Unit tests** for `elt-common` go in `elt-common/tests/` and use `pytest`.
- **End-to-end tests** for ingestion scripts go alongside the script in a `tests/`
  subdirectory (e.g. `warehouses/facility_ops_landing/ingest/accelerator/opralogweb/tests/`).
- **dbt tests** are defined in the dbt project under `warehouses/facility_ops/transform/tests/`
  and run with `dbt test`.
