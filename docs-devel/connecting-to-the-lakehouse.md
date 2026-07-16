# Connecting to the Lakehouse

You can connect general-purpose SQL clients directly to the Lakehouse for ad-hoc querying and debugging.
This page collects the supported methods.

There are two broad approaches:

- **Via Trino** - connect a JDBC/ODBC SQL client to the Trino query engine, which in turn reads
  Iceberg tables through the Lakekeeper catalog. This is the recommended approach for most
  interactive querying. [DBeaver](#dbeaver) is documented below.
- **Direct catalog attachment** - some engines (e.g. [DuckDB](https://duckdb.org)) can attach
  directly to the Iceberg REST catalog exposed by Lakekeeper, bypassing Trino entirely.

## DBeaver

[DBeaver](https://dbeaver.io) ships with a built-in Trino driver, so no manual JDBC driver
installation is required.

You can create a separate connection for each deployment method: local, dev, qa.

### 1. Create a new connection

1. **Database** > **New Database Connection**.
2. Search for and select **Trino**, then click **Next**.

### 2. Configure the connection

On the **Main** tab:

| Field | Value (local docker) | Value (remote) |
| --- | --- | --- |
| Host | `localhost` | \<domain name\> |
| Port | `58443` | `8443` |
| Username | value of `TRINO_USER` from `infra/local/env-local` | value from `trino_users` in [Vault](./deployment/prerequisites.md#vault-access) |
| Password | value of `TRINO_PASSWORD` from `infra/local/env-local` | value from `trino_users` in [Vault](./deployment/prerequisites.md#vault-access) |

Domain names for the remote systems can be found in the Ansible inventories:

- [qa](../infra/ansible/inventories/qa/group_vars/all/all.yml)
- [dev](../infra/ansible/inventories/dev/group_vars/all/all.yml)

### 3. Configure SSL/TLS

Trino requires HTTPS when authentication is enabled.
On the **Driver properties** tab set `SSL=true`.

_For the local stack only_: On the **Driver properties** tab add a new property `SSLVerification=NONE`,
which is the JDBC-driver equivalent of the `--insecure` flag used by the `trino` CLI.
_Note: This disables both hostname and trust-chain verification. This is fine
for local development but should never be used against a production endpoint._

### 4. Test and finish

Click **Test Connection...**. You should see a successful connection message. Click **Finish**.

### 5. Run a query

Open a SQL editor against the connection and run, for example:

```sql
SELECT * FROM facility_ops.analytics_accelerator.cycles;
```

## DuckDB

[DuckDB](https://duckdb.org) supports Iceberg REST catalogs using its [`ATTACH` statement](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs).
[`infra/scripts/duckdb-attach-lakehouses.sh`](../infra/scripts/duckdb-attach-lakehouses.sh) launches an interactive DuckDB session
already attached to the `facility_ops_landing` and `facility_ops` warehouses of a given
environment, without persisting any credentials to disk.

### Local docker

All access credentials are available locally, run (from the `infra` directory):

```bash
>./scripts/duckdb-attach-lakehouses.sh local
DuckDB v1.5.4 (Variegata)
Enter ".help" for usage hints.
memory D .databases;
```

### Remote

A Vault token is required for the script to access the required secrets.
See [deployment instructions](./deployment/index#services) on retrieving a token.

```bash
>VAULT_TOKEN=$(cat ansible/.vault_token) ./scripts/duckdb-attach-lakehouses.sh <qa|dev>
DuckDB v1.5.4 (Variegata)
Enter ".help" for usage hints.
memory D .databases;
```

where `<qa|dev>` should equal one of the options in the bracket.
