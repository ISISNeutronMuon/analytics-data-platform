# ELT Pipeline Development

This guide describes how the ELT (Extract, Load, Transform) pipelines are structured and
how to add new data sources or transformations.

## Overview

The platform follows a two-stage ELT pattern:

1. **Ingestion** (Extract & Load) — Python scripts using `dlt` and `elt-common` load raw
   data into the `facility_ops_landing` catalog (landing layer).
2. **Transformation** — dbt models in the `facility_ops` catalog read from the landing
   tables and produce cleaned, modelled tables (analytics layer).

These stages are separated into distinct Lakekeeper warehouses (catalogs). See
[catalogs](./data-architecture/catalogs.md) for the full catalog structure.

## Directory structure

```text
warehouses/
├── facility_ops_landing/       # Ingestion (landing)
│   └── ingest/
│       ├── domain-1/
│           └── source-1/
│       ├── domain-2/
│       │   ├── source-1/
└── facility_ops/               # Transformation (analytics)
    └── transform/
        ├── dbt_project.yml
        ├── profiles.yml
        ├── models/
        │   ├── staging/        # Staging models (light cleaning)
        │   │   ├── source-domain-1/
        │   │   └── source-domain-2/
        │   └── marts/          # Mart models (analytics-ready, non-aggregated)
        │       ├── consumer-domain-1/
        │       └── consumer-domain-2/
        ├── tests/
        └── macros/
```

## Adding a new ingestion source

### 1. Create the source directory

Create a new directory under the appropriate domain in `warehouses/facility_ops_landing/ingest/`:

```bash
mkdir -p warehouses/facility_ops_landing/ingest/<domain>/<source_name>
```

### 2. Write the ingestion script

Create a Python script that uses `dlt` and the `elt-common` helpers to extract data from
the source and load it into Iceberg tables. Look at the current examples and follow the same
pattern.

Key conventions:

- Use `elt-common` helpers where possible for consistency (see `elt-common/README.md`).
- The Iceberg namespace (schema) name is formed from `<pipline_name>_<source_domain>`.
- Add a inline script metadata for dependencies if using `uv run`.

### 3. Test the ingestion

With the local services running (see [Getting Started](./getting-started.md)):

```bash
cd warehouses/facility_ops_landing/ingest/<domain>/<source_name>
python <script_name>.py
```

Verify the tables appeared in the [Lakekeeper UI](http://localhost:50080/iceberg/ui/warehouse).

## Adding dbt transformation models

### 1. Add staging models

Create SQL models under `warehouses/facility_ops/transform/models/staging/<domain>/`.
Staging models apply light cleaning and column renaming on top of the raw landing tables.
Follow the existing examples.

### 2. Add mart models

Create analytics-ready models under `warehouses/facility_ops/transform/models/marts/<domain>/`.
Mart models join, apply business logic and optionally aggregate.

### 3. Run and test the models

```bash
cd warehouses/facility_ops/transform
dbt run                                     # Run all models
dbt run --select '+models/marts/<domain>/'  # Run a subset with dependencies
dbt test                                    # Run data quality tests
```

To run against a remote (deployed) Trino instance, set the environment variables
defined in `profiles.yml` and use:

```bash
dbt run --profile remote
```

See the [transform README](../warehouses/facility_ops/transform/README.md) for additional details.

## The elt-common package

[elt-common](../elt-common/) is a shared Python package built on top of `dlt` that
provides common functionality used across ingestion scripts. It is not published
to PyPI — install it from the repository source:

```bash
uv pip install "elt-common @ git+https://github.com/ISISNeutronMuon/analytics-data-platform.git#subdirectory=elt-common"
```

For local development of `elt-common` itself, see the
[Getting Started](./getting-started.md#set-up-elt-common-for-development) guide.
