# 11. Replace DLT in ingestion pipelines

Date: 2026-06-25

## Status

Accepted

Supercedes [10. Use data load tool for ingesting data](0010-use-data-load-tool-for-ingesting-data.md)

## Context

The data load tool (DLT) framework has been used to develop the initial set of pipelines for the data platform. However,
it's proved to have several drawbacks:

- It processes things in a heavier-weight way than we really need, writing all data to files before uploading them
- It uses decorators heavily, making debugging difficult
- Upgrades that should have been compatible have occasionally broken pipelines
- It caches data in sometimes unobvious ways, which can lead to unexpected results when developing pipelines
- The way it ingests some data makes it more complex to transform in later steps

Additionally, it both provides a lot of functionality we _don't_ need, and doesn't include some functionality that we
_do_. This has led to us having to write integrations for a couple of the more complicated parts of the pipelines
(extracting from SharePoint and uploading to Iceberg), and making them work as DLT extensions adds an additional layer
of complexity.

## Decision

We will write a minimal framework to support the functionality/abstractions we need, and rework the existing pipelines
to use it.

The new framework will provide a CLI which can be used to 'run' a pipeline script, replacing the current setup where
each script is a standalone script designed to be [run with uv](https://docs.astral.sh/uv/guides/scripts/). The new
pipelines will still be Python files, but will integrate with the new framework by implementing an abstract base class
from the framework. The pipeline script will define how to fetch data, and some properties for how it should be uploaded
whilst the framework will handle orchestration, and actually executing the upload(s).

Each pipeline script will define how data is extracted from a single data source, but could upload data to more than one
Iceberg table.

We'll continue to use [`pydantic-settings`](https://pypi.org/project/pydantic-settings/) as the main configuration
mechanism, and each pipeline will optionally be able to declare any custom configuration it requires.

The pipelines will be part of a python project which provides the shared dependencies (including the new framework).
Individual scripts may have additional dependencies, which can be added as
[optional-dependencies](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/#dependencies-optional-dependencies).

## Consequences

We will have to write and maintain the framework code, which is a nontrivial amount of work. However, long term it gives
us complete control over the outcome, and will likely make development simpler.
