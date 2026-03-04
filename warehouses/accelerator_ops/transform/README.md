# Accelerator catalog models

[dbt](https://docs.getdbt.com/) projects for defining models within the `accelerator` catalog.

## One-time setup

Install the Python requirements into a virtual environment:

```bash
>uv pip install -r ./requirements/requirements.txt
```

For running against the local, docker-based setup see [infra/local/README](../../../../infra/local/README.md#one-time-setup)
to configure a `local_catalog` dbt profile. Ensure the docker services are running following the
instructions in that file.

Run the models against the local catalog:

```bash
dbt --profiles-dir ~/.dbt --profile local_catalog run
```
