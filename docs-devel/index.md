# Analytics Data Platform

## Introduction

After requirements gathering meetings and a workshop as part of the
[accelerator operational data platform project](https://stfc365.sharepoint.com/sites/ISISProject-1432)
there is appetite for trialling a system to support long-term storage, complex
queries and analysis of data from a multitude of sources across the accelerator
data space.

This document describes the initial set of requirements along with an overview
of the design and implementation of a minimal system to prove
the [Data Lakehouse](https://stfc365-my.sharepoint.com/:w:/g/personal/martyn_gigg_stfc_ac_uk/ETTFvkI6FulFhQFWmrmfGosBRO1Syvqbiq6DhVwnqxhVbw?e=HNKUIl)
concept can be valuable to ISIS.

## Repository overview

This repository is a monorepo containing all of the code for the platform. It may be separated out in
the future.

```text
.
├── certs/                  # Certificate request configurations used for HTTPS/SSL
├── elt-common/             # Reusable Python package with common ETL/ELT helpers used by the warehouses
├── docs/                   # User and developer documentation using MkDocs. See `docs/src` for content used in the published docs site.
├── infra/
│   ├── ansible/            # Ansible playbooks/roles to deploy the system to the STFC (OpenStack) cloud.
│   ├── container-images/   # Container definitions deployed services
│   └── local/              # docker-compose configuration for a local development environment and end-to-end CI tests.
└── warehouses/             # One subdirectory per (Lakekeeper) warehouse. Each contains ELT code to extract, transform and load data from external sources into Iceberg tables.
```

## Details

Jump to:

- [System architecture](./system-architecture/index.md)
- [Data architecture](./data-architecture/index.md)
- [Deployment](./deployment/index.md)
