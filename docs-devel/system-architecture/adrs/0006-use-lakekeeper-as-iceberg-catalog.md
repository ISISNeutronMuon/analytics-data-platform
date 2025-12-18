# 6. Use Lakekeeper as the Iceberg Catalog

## Status

Accepted

## Context

[Apache Iceberg](./0005-use-apache-iceberg-as-lakehouse-table-format.md) requires a
[catalog](https://iceberg.apache.org/terms/#catalog) to track table metadata.
Several implementations exist within the Iceberg libraries including:

- REST: a server-side catalog that’s exposed through a REST API
- Hive Metastore: tracks namespaces and tables using a Hive metastore
- JDBC: tracks namespaces and tables in a simple JDBC database
- Nessie: a transactional catalog that tracks namespaces and tables in a database with git-like version control

The REST catalog requires a server to implement the specification but offers more flexibility without
have to modify the Iceberg libraries, e.g a query engine using REST doesn't need to be modified to
switch REST catalog implementations.

## Decision

The REST specification offers greater flexibility in how clients interact with the catalog and several
implementations exist that support the Iceberg REST spec:

- [Apache Gravitino](https://gravitino.apache.org/): Java based, offering federation across multiple data catalogs,
  including support for authentication and access management.
- [Apache Polaris](https://polaris.apache.org/): Java based incubating Apache project to provide an alternative
  implementation of the Iceberg REST catalog implementation. Dremio [announced](https://projectnessie.org/blog/2024/08/02/open-source-polaris-announcement/)
  they would look to add Nessie-like features to Polaris.
- [Lakekeeper](https://docs.lakekeeper.io/): Newer, Rust-based catalog providing multiple warehouse support, access control, a basic UI for catalog management.
  Good documentation and examples, simple to deploy.
- [Project Nessie](https://projectnessie.org/): Java-based catalog supporting aiming to bring version-control features like branching, merging to the catalog as a whole.
  Interesting idea but through a basic trial the [catalog maintenance](https://iceberg.apache.org/docs/nightly/nessie/#further-use-cases) was found to
  be complicated using Nessie. What will the future of Nessie be?

We will choose Lakekeeper as a REST-based catalog server due to:

- Ease of deployment: a single binary.
- Excellent documentation and examples of different configurations.
- Manage data access centrally in the catalog through emerging open standards such as OpenFGA and Open Policy Agent.
- Built-in support for external secret stores such as Hashicorp-Vault.
- Event publishing to NATS, Kafka.
- Lower resource footprint by using Rust.

## Consequences

Lakekeeper will be the back bone of the data lake and is a relatively new project so there is some
risk that it does not survive.
The design of Apache Iceberg mitigates the risk of migrating to a new catalog if that becomes
necessary as the catalog only stores metadata and would not require migrating the data on the object
store.
