# 10. Use data load tool for ingesting data

Date: 2026-06-25

## Status

Superceded by [11. Replace DLT in ingestion pipelines](0011-replace-dlt-in-ingestion-pipelines.md)

## Context

We ingest data from many different data sources, but there are significant similarities between all of the ingestion
pipelines; they all 'read the data', (possibly) 'clean the data', then 'upload the data'. It'd be useful to have a tool
which abstracts the shared parts of those pipelines to simplify their development and maintenance.

## Decision

We will use the [`data-load-tool`](https://github.com/dlt-hub/dlt) (DLT) framework for our ingestion pipelines.

## Consequences

DLT provides the required abstractions for the shared parts of the data pipelines, substantially reducing the amount of
code we'll need to write. It's a widely used frameworok supporting a huge amount of data sources, and providing a lot of
functionality that could be useful for us.

An obvious downside is that DLT doesn't have builtin support for Iceberg as a destination for the data. However, it is
extensible, so we can write our own integration; similarly, it's possible it won't have support for all of the data
sources we need to read from, but again provides extension points which we could use to cover any gaps if required.
