# Analytics Data Platform

> **Audience**: This is the *developer* documentation for contributors to the platform.
> For user-facing documentation (Superset guides, data dictionaries, etc.) see the
> published site built from `docs/`.

## Introduction

An Apache Iceberg-based data lakehouse to support analytics for the facility.
See [Background](./background.md) for a high-level overview of a data lakehouse.

## Repository overview

This repository is a monorepo containing all of the code for the platform. It may be separated out in
the future.

```text
.
├── certs/                  # Certificate request configurations used for HTTPS/SSL
├── docs/                   # User-facing documentation site (MkDocs). See docs/src for content.
├── docs-devel/             # Developer documentation (this directory).
├── elt-common/             # Reusable Python package with common ELT helpers used by the warehouses
├── infra/
│   ├── ansible/            # Ansible playbooks/roles to deploy the system to the STFC (OpenStack) cloud.
│   ├── container-images/   # Container definitions for deployed services
│   └── local/              # docker-compose configuration for local development and end-to-end CI tests.
└── warehouses/             # One subdirectory per Lakekeeper warehouse. Each contains ELT code for that warehouse.
    ├── facility_ops_landing/   # Ingestion scripts (bronze layer)
    └── facility_ops/           # dbt transformation models (silver/gold layers)
```

## Getting started

New to the project? Start with the [Getting Started](./getting-started.md) guide to set up
your local development environment.

## Details

- [Getting started](./getting-started.md) — local setup, first pipeline run
- [System architecture](./system-architecture/index.md) — services, tools, ADRs
- [Data architecture](./data-architecture/index.md) — medallion layout, catalogs
- [ELT pipeline development](./elt-pipelines.md) — how to build and modify pipelines
- [CI/CD and testing](./testing.md) — running tests, CI workflows
- [Deployment](./deployment/index.md) — cloud provisioning and service deployment
- [Contributing](./contributing.md) — branching, PRs, code style
