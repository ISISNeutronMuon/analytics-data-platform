# Catalogs

This page describes the catalogs (Lakekeeper warehouses) that exist within the ISIS Lakehouse.
Each catalog maps to a subdirectory under `warehouses/` in the repository.

## Index

### `facility_ops_landing`

The landing catalog for raw ingested data (landing layer). Data is pushed into Iceberg tables
via `dlt`-based ingestion scripts with minimal transformation.

- **Source code**: `warehouses/facility_ops_landing/ingest/`
- **Data domains**: Each subdirectory of the above ingest directory defines a source domain
- **Schemas**: Each data source gets its own schema, e.g. `accelerator_statusdisplay`, `accelerator_opralogweb`.

### `facility_ops`

The analytics catalog for cleaned, validated and modelled data (analytics layer).
Transformations are defined as dbt models that are read from `facility_ops_landing` and produce
curated tables.

- **Source code**: `warehouses/facility_ops/transform/`
- **Model domains**: Each subdirectory of `transform/models/marts` defines the consumer domain.
- **Schemas**: Each consumer defines the suffix for the schema name e.g. `analytics_accelerator`.
