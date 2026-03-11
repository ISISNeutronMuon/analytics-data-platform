# Data Architecture

These pages describe our implementation of the Data Lakehouse using a
[Medallion-style layout](https://docs.databricks.com/aws/en/lakehouse/medallion)
to organise data as it flows from source systems through to analytics consumers.

## High-level view

We deviate from the standard layer names, `bronze`/`silver`/`gold`, and instead use `landing`/`analytics`
for clarity. We do not currently have a `gold`-like layer.

### Landing

The landing zone for ingested data. Data is pushed into Iceberg tables either in its original state
or minimally cleaned/normalised. The landing layer is the single source of truth for all subsequent
transformations and supports full reprocessing to regenerate the tables in the subsequent layers.
This layer is not intended for use by end consumers.

### Analytics

Data is cleaned and validated, including enforcing quality checks. The analytics layer typically
applies little to no aggregation leaving detailed information for end users to perform their
own analyses.

An example diagram of this architecture for a business operations domain is reproduced here:

![Example medallion architecture](../images/medallion-architecture.png)
_[Source: Databricks](https://docs.databricks.com/aws/en/lakehouse/medallion)_

For a description of the implementation of this architecture see the
[system architecture](../system-architecture/index.md) documentation.

For a description of the structure of the catalogs for ISIS see [catalogs](./catalogs.md).
