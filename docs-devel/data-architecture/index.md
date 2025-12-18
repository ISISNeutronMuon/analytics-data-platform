# Data Architecture

These pages describe our implementation of the Data Lakehouse using a
[Medallion-style layout](https://docs.databricks.com/aws/en/lakehouse/medallion)
to organise data as it flows from source systems through to analytics consumers.

## High-level view

We use the standard layer names `bronze`/`silver`/`gold` where each layer has a clear responsibility. See diagram below

### Bronze

The landing zone for ingested data. Data is pushed into Iceberg tables either in its original state
or minimally cleaned/normalised. The bronze layer is the single source of truth for all subsequent
transformations and supports full reprocessing to regenerate the tables in the subsequent layers.
This layer is not intended for use by end consumers.

### Silver

Data is cleaned and validated, including enforcing quality checks. The silver layer typically
applies little to no aggregation leaving detailed information for end users to perform their
own analyses.

### Gold

Data is curated, typically with aggregation applied, into consumption-ready datasets built for business intelligence (BI), reporting and machine learning.

An example diagram of this architecture for a business operations domain is reproduced here:

![Example medallion architecture](https://docs.databricks.com/aws/en/assets/images/medallion-architecture-15e2d57ad70d28b1701dda4f7271d862.png)
_[Source: Databricks](https://docs.databricks.com/aws/en/assets/images/medallion-architecture-15e2d57ad70d28b1701dda4f7271d862.png)_

For a description of the implementation of this architecture see the [system architecture](../system-architecture/index.md) documentation.

For a description of the structure of the catalogs for ISIS see [catalogs](./catalogs.md).
