# Architecture Decision Records

See [0001-record-architecture-decisions.md](./0001-record-architecture-decisions.md)
for details on why we use ADRs.

## Index

| ADR                                                                 | Title                                                  | Status     |
|---------------------------------------------------------------------|--------------------------------------------------------|------------|
| [0001](./0001-record-architecture-decisions.md)                     | Record architecture decisions                          | Accepted   |
| [0002](./0002-use-ansible-for-vm-provisioning-and-configuration.md) | Use Ansible for VM provisioning and configuration      | Accepted   |
| [0003](./0003-use-scd-cloud-vms.md)                                 | Use SCD Cloud VMs                                      | Accepted   |
| [0004](./0004-use-minio-as-object-store.md)                         | Use MinIO as object store                              | Superseded |
| [0005](./0005-use-apache-iceberg-as-lakehouse-table-format.md)      | Use Apache Iceberg as Lakehouse table format           | Accepted   |
| [0006](./0006-use-lakekeeper-as-iceberg-catalog.md)                 | Use Lakekeeper as the Iceberg catalog                  | Accepted   |
| [0007](./0007-use-echo-s3-object-store.md)                          | Use Echo as S3 object store                            | Accepted   |
| [0008](./0008-use-superset-as-reporting-tool-frontend.md)           | Use Superset as frontend replacement of reporting-tool | Accepted   |
| [0009](./0009-use-apache-airflow-as-job-orchestration-tool.md)      | Use Apache Airflow as job orchestration tool           | Accepted   |
| [00010](./0010-use-data-load-tool-for-ingesting-data.md)            | Use data load tool for ingesting data                  | Superseded |
| [00011](./0011-replace-dlt-in-ingestion-pipelines.md)               | Replace DLT in ingestion pipelines                     | Accepted   |

## adr-tools

To install the ADR tools see the
[quickstart guide](https://github.com/npryce/adr-tools?tab=readme-ov-file#quick-start).
