# Facility Operations catalog models

[dbt](https://docs.getdbt.com/) projects for defining models within the `facility_ops` catalog.

## One-time setup

Install the Python requirements into a virtual environment:

```bash
> uv pip install -r ./requirements/requirements.txt
```

Run the models against a local catalog, ensure the docker services in
[infra/local/README](../../../../infra/local/README.md#one-time-setup) are running:

```bash
dbt run
```

Running the models against a remote catalog requires environment variables to be set to
point to the Trino instance, see [profiles.yml](./profiles.yml) for the required variables.
Once defined run:

```bash
dbt run --profile remote
```
