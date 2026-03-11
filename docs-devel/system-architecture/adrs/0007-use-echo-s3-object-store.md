# 4. Use Ceph S3 as object store

## Status

Accepted

## Context

A lakehouse uses an object store for all structured, semi-structured and unstructured data.

## Decision

Use object storage on the SCD storage platform that has been procured for production/QA
workloads. The development deployment will still use a MinIO service run by use using docker.

## Consequences

No longer have to worry about managing the object storage for production workloads. Note that the
storage is not backed up as our current assumption is all data exists in source systems
we ingest form and our system can be rebuilt if necessary.
