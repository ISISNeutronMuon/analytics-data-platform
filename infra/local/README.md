# Development infrastructure

The services defined here are intended for local development and should not be used for production.
We do not use https for local development, with the exception of Trino that requires it, due to
complications of ensuring self-signed certificates are trusted correctly across the host and
all service containers.
*Repeat: This configuration should not be used in production.*.

## One-time setup

An additional entry in `/etc/hosts` on the host machine is required:

```sh
127.0.0.1    adp-router
```

Some tools, such as PyIceberg and Superset in the browser, need to access services both outside
and inside the container network. The `adp-router` service is a Traefik instance that routes
traffic to match the url layout of the production deployment. By defining the above `/etc/hosts`
entry a request will hit the router regardless of whether it is inside or outside of the container
network.

To test ELT scripts based on the Python package `dlt`, create and add the following to
`$HOME/.dlt/secrets.toml`:

```toml
[destination.pyiceberg.credentials]
uri = "http://localhost:50080/iceberg/catalog"
warehouse = "facility_ops_landing"
oauth2_server_uri = "http://localhost:50080/auth/realms/analytics-data-platform/protocol/openid-connect/token"
client_id = "machine-infra"
client_secret = "s3cr3t"
scope = "lakekeeper"
```

## Start services

Bring up the services with `docker compose`:

```sh
docker compose --profile superset up --wait
```

Services reference:

- Keycloak identity provider: <http://localhost:50080/auth>
   - `master` realm credentials: *admin/admin*
   - `analytics-data-platform` realm credentials: *adpsuperuser/adppassword*
- Lakekeeper iceberg catalog UI: <http://localhost:50080/iceberg/ui>.
   - Use the same credentials as the `analytics-data-platform` realm above.
- Superset BI tool: <http://localhost:50080/workspace/facility_ops>.
   - Use the same credentials as the `analytics-data-platform` realm above.
- Trino endpoint: <https://localhost:58443> (note the https as this is required by Trino. You may need to use an "--insecure" flag)
- Marimo notebook server: <http://localhost:50080/marimo/>

## Example: Run an ingestion script and then transformation

From the root of the repository run the ingestion script:

```bash
> cd warehouses/facility_ops_landing/ingest/accelerator/statusdisplay
> python statusdisplay.py
```

Login to <http://localhost:50080/iceberg/ui/warehouse> and check the `facility_ops_landing`
warehouse contains tables in the `accelerator_statusdisplay` namespace.

Now try the transformation on the models based on this dataset:

```bash
> cd warehouses/facility_ops/transform
> dbt run --select '+models/marts/accelerator/cycles.sql'
```

You will see warnings like:

  InsecureRequestWarning: Unverified HTTPS request is being made to host 'localhost'. Adding certificate verification is strongly advised. See: <https://urllib3.readthedocs.io/en/latest/advanced-usage.html#tls-warnings>

when running locally, this is expected. It should not occur in production.

Login to <http://localhost:50080/workspace/facility_ops>. Select the "SQL Lab" option
under the SQL tab and run

```sql
SELECT * FROM facility_ops.analytics_accelerator.cycles
```
