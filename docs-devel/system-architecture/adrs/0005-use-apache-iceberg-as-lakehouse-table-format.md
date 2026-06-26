# 5. Use Apache Iceberg as Lakehouse Table Format

Date: 2024-07-04

## Status

Accepted

## Context

An open table format is one of the foundations of a Lakehouse architecture.
Popular options include:

- [Apache Iceberg](https://iceberg.apache.org/)
- [Delta Lake](https://delta.io/)
- [Apache Hudi](https://hudi.apache.org/)

## Decision

We will use the Apache Iceberg format for our lakehouse proof-of-concept.
It is emerging as a leader in the field of table formats and in particular support for "hidden partitioning", where partitions are maintained by Iceberg and do not have to be managed by the user seems advantageous.

## Consequences

We'll gain experience of the Iceberg format and can still make a decision if
the format would be best for a final system.
