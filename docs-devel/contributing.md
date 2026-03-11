# Contributing

Guidelines for contributing to the Analytics Data Platform.

## Development setup

See the [Getting Started](./getting-started.md) guide for setting up your local
development environment.

## Branching strategy

- `main` is the primary branch. All work is merged into `main` via pull requests.
- Create feature branches from `main` with descriptive names (e.g.
  `add-beam-monitors-ingestion`, `fix-cycles-model-join`).

## Pull requests

### Title conventions

PR titles must follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):

```text
type(scope): description
```

**Types**: `fix`, `feat`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`

**Examples**:

- `fix(warehouse/facility_ops): fix join in cycles model`
- `feat(elt-common): add retry logic for HTTP sources`
- `docs(docs-devel): update deployment prerequisites`
- `chore(infra): bump Keycloak version`

### PR template

The repository includes a PR template at `.github/PULL_REQUEST_TEMPLATE.md`. When creating
a PR, fill in the summary section and link any related issues.

### Review process

- All PRs require at least one approving review before merge.
- CI checks (static analysis, tests) must pass.
- Keep PRs focused — prefer smaller, incremental changes over large monolithic PRs.

## Code style

- **Python**: Formatting and linting are enforced by `prek` (pre-commit hooks). Run
  `prek run --all-files` to check locally before pushing.
- **SQL (dbt)**: Follow the existing model naming conventions:
   - Staging models: `stg_<source>__<table>.sql`
   - Mart models: descriptive names reflecting the business concept (e.g. `cycles.sql`).
- **Ansible**: Follow the role/playbook structure established in `infra/ansible/`.

## Architecture Decision Records

Significant technology or design decisions are recorded as ADRs in
`docs-devel/system-architecture/adrs/`. If your change introduces a significant
architectural decision, add a new ADR using `adr-tools`:

```bash
adr new "Use X for Y"
```

See the [ADR index](./system-architecture/adrs/index.md) for existing decisions.
