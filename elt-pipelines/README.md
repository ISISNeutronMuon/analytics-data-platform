# elt-pipelines

Pipelines for ingesting data from various sources into Iceberg catalogs using elt-common.

**Under construction** - this project is being developed as a replacement for the existing DLT pipelines.
[See here](https://github.com/ISISNeutronMuon/analytics-data-platform/issues/321) for details.

## Development setup

Development requires the following tools:

- [uv](https://docs.astral.uv/uv/): Used to manage both Python installations and dependencies

### Setting up a Python virtual environment

Once `uv` is installed, create an environment, activate it, and install dependencies with:

```bash
> uv venv
> source .venv/bin/activate
> uv sync
```

Pipelines can declare optional dependencies in `pyproject.toml` - for example, `statusdisplay` uses `requests` for
fetching data. To install any additional dependencies for that specific pipeline, use:

```bash
> uv sync --extra statusdisplay
```

## Running a pipeline

Pipelines are run using the `elt` CLI tool. As an example, with `elt-pipelines` as the current working directory,
`elt run facility_ops statusdisplay` will run the statusdisplay pipeline. See `elt -h` for full usage.

## Directory structure

The project uses the following directory structure:

```txt
elt-pipelines/
|-- <target warehouse>/
|    |-- ingest/
|    |    |-- <domain>/
|    |    |    |-- <job name>/
|    |    |    |    |-- <job name>.py
```

- This directory structure is required for using `elt-common`
- Each 'target warehouse' is the name of an Iceberg warehouse. The data ingested by the pipelines inside that directory
  end up in that warehouse.
- Data from ingest pipelines is considered 'raw' data, and is loaded into a warehouse suffixed with `_landing`.
- Under construction: Each warehouse will also have a `transform` subdirectory containing pipelines for converting the
  raw data into its final state in the target warehouse.
