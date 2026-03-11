# Getting Started

This guide walks you through setting up a local development environment and running
your first end-to-end pipeline.

_Note: The steps defined here assume using a Linux-like terminal. On Windows you'll need to use WSL._

## Prerequisites

Install the following tools before proceeding:

| Tool | Purpose | Install guide |
| --- | --- | --- |
| Docker | Runs the local service stack | Ensure at least 4 CPU / 8 GB RAM allocated |
| [uv](https://docs.astral.sh/uv/) | Python and virtual environment management | [Installation](https://docs.astral.sh/uv/getting-started/installation/) |
| [prek](https://pypi.org/project/prek/) | Pre-commit hooks / static checks | `pip install prek` |
| [Git](https://git-scm.com/) | Version control | Your OS package manager |

## Clone the repository

```bash
git clone git@github.com:ISISNeutronMuon/analytics-data-platform.git
cd analytics-data-platform
```

## Install uv-managed Python interpreter

The Python version is defined in `elt-common/pyproject.toml`, use this to install the correct
version of Python:

```bash
cd elt-common
uv python install
cd ..
```

## Setup a Python environment

From the root of the repository run:

```bash
uv venv
```

Run `source .venv/bin/activate` to activate the Python environment in the current shell.
Install the `elt-common` package and dev dependencies in editable mode:

```bash
cd elt-common
uv pip install --editable . --group dev
```

See [elt-common/README.md](../elt-common/README.md) for more details.

## Install pre-commit hooks

```bash
prek install
```

Check prek runs successfully:

```bash
prek run --all-files
```

## Setup /etc/hosts

Add the following entry to `/etc/hosts` on your machine:

```bash
127.0.0.1    adp-router
```

_Explanation_: Some tools (PyIceberg, Superset in the browser) need to resolve service URLs consistently
inside and outside the Docker container network. The `adp-router` service is defined in docker-compose
and points to a Traefik instance that routes traffic to the appropriate service.
This definition means the `adp-router` domain on the host points to the local machine as it does
inside the docker network.

## Configure dlt secrets

Create (or append to) `$HOME/.dlt/secrets.toml` with the local development credentials:

```toml
[destination.pyiceberg.credentials]
uri = "http://localhost:50080/iceberg/catalog"
warehouse = "facility_ops_landing"
oauth2_server_uri = "http://localhost:50080/auth/realms/analytics-data-platform/protocol/openid-connect/token"
client_id = "machine-infra"
client_secret = "s3cr3t"
scope = "lakekeeper"
```

## Start the local service stack

Bring up all services with Docker Compose:

```bash
cd infra/local
docker compose --profile superset up --wait
```

Once running, the following services are available:

| Service | URL | Credentials |
| --- | --- | --- |
| Keycloak (master realm) | <http://localhost:50080/auth> | admin / admin |
| Lakekeeper UI | <http://localhost:50080/iceberg/ui> | adpsuperuser / adppassword |
| Superset | <http://localhost:50080/workspace/facility_ops> | adpsuperuser / adppassword |
| Trino | <https://localhost:58443> | (use `--insecure` flag) |
| Marimo notebooks | <http://localhost:50080/marimo/> | — |

## Run your first pipeline

### 1. Ingest data (landing layer)

From the repository root, run an ingestion script:

```bash
cd warehouses/facility_ops_landing/ingest/accelerator/statusdisplay
python statusdisplay.py
```

Verify the data landed by logging into the
[Lakekeeper UI](http://localhost:50080/iceberg/ui/warehouse) and checking that the
`facility_ops_landing` warehouse contains tables in the `accelerator_statusdisplay` namespace.

### 2. Transform data (analytics layer)

Run the dbt models that depend on the ingested data:

```bash
cd warehouses/facility_ops/transform
uv pip install -r ./requirements/requirements.txt
dbt run --select '+models/marts/accelerator/cycles.sql'
```

You will see `InsecureRequestWarning` messages — this is expected locally due to
the self-signed Trino certificate.

### 3. Query the results

Log in to [Superset](http://localhost:50080/workspace/facility_ops), open
**SQL Lab** from the SQL tab, and run:

```sql
SELECT * FROM facility_ops.analytics_accelerator.cycles
```

## Next steps

- Read the [ELT pipeline development](./elt-pipelines.md) guide to understand how to
  add new data sources and transformations.
- Review the [system architecture](./system-architecture/index.md) to understand how
  the services fit together.
- See [contributing](./contributing.md) for branching and PR conventions.
