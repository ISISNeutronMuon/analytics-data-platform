# LLM Context Guide for Analytics Data Platform

This repository contains ELT code for an Iceberg-based data warehouse, together with
infrastructure code used to run the warehouse both locally (docker-compose) and in an
OpenStack cloud (Ansible).

Use this document as a concise, developer-friendly reference when working with the
repository or providing context to an assistant/agent about the project structure and
common developer tasks.

## Purpose

- Give human developers and LLM-based agents the essential repository layout and
  pointers for local development, testing and deployment.
- Point to the configuration locations used for certificates, local environments,
  and deployable Ansible playbooks/roles.

## Repository layout (high level)

- [`docs-devel/src/index.md#repository-overview`](./docs-devel/src/index.md#repository-overview) describes the repository layout.

## Pre-commit hooks

- Static code checks are enforced using `prek`. Install with your environment's tooling and run checks
  on all files with `prek run --all-files`. Install the pre-commit hook with `prek install`.

## Running tests

- There are both unit and e2e tests for the `elt-common` package under `elt-common/tests/`. They are
  written using `pytest`.
- [`elt-common/README.md`](./elt-common/README.md) explains how to run tests for the `elt-common`
  Python package

## Cloud deployment

- Use the Ansible playbooks in `infra/ansible-docker/` together with the
  `inventory*.yml` files. See the `infra/ansible-docker/readme.md` for role and
  variable guidance.

## Pull Request Guidelines

- When creating pull requests:

1. **Read the current PR template**: Always check `.github/PULL_REQUEST_TEMPLATE.md` for the latest format

2. **Follow PR title conventions**: Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
   - Format: `type(scope): description`
   - Example: `fix(warehouse/accelerator): fix join in model`
   - Types: `fix`, `feat`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`

**Important**: Always reference the actual template file at `.github/PULL_REQUEST_TEMPLATE.md` instead of using cached content, as the template may be updated over time.

## Troubleshooting & tips

- Docker resource issues: the local compose stack can be resource heavy. Ensure
  Docker Desktop has enough CPU/memory.
- Ansible role errors: ensure you have required galaxy roles (see
  `infra/ansible-docker/ansible-galaxy-requirements.yaml`) and the correct Python
  and Ansible versions installed.

## Where to go next

- Read `docs/` and `docs-devel/` for high-level architecture and deployment
  instructions.
- Inspect `warehouses/` for per-warehouse ELT implementations and examples.

---

This file was created to provide a concise, shareable context document for humans
and LLM-based agents working with the analytics-data-platform monorepo.
