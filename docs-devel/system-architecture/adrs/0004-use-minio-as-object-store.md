# 4. Use MinIO as object store

## Status

Accepted

## Context

A lakehouse uses an object store for all structured, semi-structured and unstructured data.
While object storage is offered by the SCD Echo service ISIS currently has no allocation of this type.

## Decision

ISIS has a large allocation of CephFS storage on Deneb.
Use [MinIO](https://min.io/) as an object store with the data persisted through
a Manila share on Openstack.
MinIO is a high-performance, S3 compatible object store built for data lake/database.

## Consequences

It is not a long term solution as it requires maintaining the object storage
layer ourselves. For a production system, should this go ahead, then some
object storage will need to be procured.
