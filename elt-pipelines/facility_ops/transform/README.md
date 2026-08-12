# Facility Operations catalog models

This is a [dbt](https://docs.getdbt.com/) project which defines transforms for turning the
raw data in the `facility_ops_landing` warehouse into cleaned models in `facility_ops`.

**Under construction. Currently only the electricity_sharepoint transforms work.**

## Running with elt

The `elt run` command defined by [`elt-common`](../../../elt-common) can be used to run
transforms from this project.

Ingest jobs ingest data into a namespace defined by the
[ingest directory structure](../../README.md#directory-structure). That namespace can be used
to run the transform(s) for the pipeline with `elt run facility_ops <namespace> --step transform`.

## Running with dbt

It's also possible to run the transforms (or interact with them in other ways) using `dbt`
directly.

To set up the environment, install the python requirements into a virtual environment:

```bash
> uv pip install -r ./requirements/requirements.txt
```

Run `dbt deps` to install the project dependencies.

Run the models against a local catalog, ensure the docker services in
[infra/local/README](../../../infra/local/README.md#local-set-up) are running:

```bash
dbt run
```

Running the models against a remote catalog requires environment variables to be set to
point to the Trino instance, see [profiles.yml](./profiles.yml) for the required variables.
Once defined run:

```bash
dbt run --profile remote
```
