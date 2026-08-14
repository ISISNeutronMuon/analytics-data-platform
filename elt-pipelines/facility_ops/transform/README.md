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

It's also possible to run the transforms (or interact with them in other ways) using `dbt` directly.

- Use a python environment with [elt-pipelines](../../README.md#setting-up-a-python-virtual-environment)
  or [elt-common](../../../elt-common/README.md#setting-up-a-python-virtual-environment) installed
   - These provide the required `dbt` dependencies, and the `dbt` cli tool
- Make the dbt project directory (`elt-pipelines/facility_ops/transform`) the working directory
- Run `dbt deps` to install the project dependencies
- For running against a local catalog, ensure the docker
  services [are running](../../../infra/local/README.md#local-set-up)
- Run `dbt` commands whilst in the dbt project directory
- To run against a remote catalog, ensure the [required environment variables](./profiles.yml) are set up to point to
  the Trino instance, then use the `--profile remote` option when running `dbt` commands
