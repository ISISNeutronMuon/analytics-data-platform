# Analytics Data Platform Proof-of-Concept

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

- [certs]: Certificate request configurations.
- [infra/local](https://github.com/ISISNeutronMuon/analytics-data-platform/tree/main/infra/local):
  Docker-compose-based configuration of the system. Used for end-to-end CI tests and local development and exploration.
- [infra/ansible-docker](https://github.com/ISISNeutronMuon/analytics-data-platform/tree/main/infra/ansible-docker):
  Ansible roles/playbooks to deploy a version of the system to the STFC cloud.
- [warehouses](https://github.com/ISISNeutronMuon/analytics-data-platform/tree/main/warehouses):
  A subdirectory per data warehouse. Each subdirectory contains ELT code to populate the warehouse
  with data extracted from external data sources and transformed to models for user consumption.
- [elt-common](https://github.com/ISISNeutronMuon/analytics-data-platform/tree/main/elt-common):
  A Python package containing common code across the ELT pipelines in the warehouses.


## Details

Jump to:

- [System architecture](./system-architecture/index.md)
- [Data architecture](./data-architecture/index.md)
- [Deployment](./deployment/index.md)
- [Development](./development/index.md)
