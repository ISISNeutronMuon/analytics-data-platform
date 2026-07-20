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

## Writing a pipeline

To create a new pipeline:

1. Create a job folder and script file following the project [directory structure](#directory-structure)
2. In the script, create a class named `Extract` which subclasses `elt_common.extract.BaseExtract`
3. Implement the `extract_resource_properties` method
    - It must be a generator that yields pairs of (`table_name`, `resource_properties`)
    - `table_name` is the name of the iceberg table the data should be loaded into
    - `resource_properties` describes how the data is extracted and loaded. See the class documentation
      [in extract.py](../elt-common/src/elt_common/extract.py) for details
4. If the pipeline requires configuration:
    - Create a class that subclasses `pydantic_settings.BaseSettings`
    - In the pipeline `Extract` class, set a class field called `config_cls` to be the configuration class
    - Add a positional argument to the `Extract` class initializer
    - An instance of the configuration will be injected as that argument, with its values instantiated as per
      pydantic_settings
    - Environment variables to populate the config values must be prefixed with `<JOBNAME>__` (where `<JOBNAME>` is the
      name of the pipeline script)

In total this looks something like this:

```python
class MyConfig(BaseSettings):
    something: str


class Extract(BaseExtract):
    config_cls = MyConfig

    def __init__(self, config):
        super().__init__(config)
        # Use any config values required for set up
        self._something = config.something

    def extract_resource_properties(self):
        yield (
            "table_name",
            ResourceProperties(
                extractor=a_function_that_extracts_the_data,
                write_properties=ResourceWriteProperties(),
                watermark_column=None
            )
        )
```

### Specialized functionality for common pipeline types

There is functionality built in to `elt_common` for some common types of pipeline. These can be found in the
[`sources` package](../elt-common/src/elt_common/sources).

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
