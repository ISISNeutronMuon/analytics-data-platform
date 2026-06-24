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

Pipelines are run using the `elt` CLI tool. As an example, with the package as current working directory,
`elt run pipelines statusdisplay` will run the statusdisplay pipeline. See `elt -h` for full usage.
