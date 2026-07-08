-- Duckdb (https://duckdb.org/) is an in-process analytics database. It's the equivalent of SQLite for analytics.
-- Amongst other capabilities is the ability to attach to Iceberg catalogs, providing a simple cli-based mechanism
-- to query the lakehouse.
--
-- Assuming the local, docker-compose stack is running, run `duckdb -init duckdb_attach.sql` to start a cli session
-- and interact with the tables from the catalogs:
--
-- >SHOW SCHEMAS;

SET s3_access_key_id='adpsuperuser';
SET s3_secret_access_key='adppassword';
SET s3_endpoint='http://adp-router:59000';

CREATE SECRET lakekeeper_secret (
    TYPE iceberg,
    CLIENT_ID 'machine-infra',
    CLIENT_SECRET 's3cr3t',
    OAUTH2_SCOPE 'lakekeeper',
    OAUTH2_SERVER_URI 'http://localhost:50080/auth/realms/analytics-data-platform/protocol/openid-connect/token'
);

ATTACH 'facility_ops_landing' AS facility_ops_landing (
    TYPE iceberg,
    ENDPOINT 'http://localhost:50080/iceberg/catalog',
    SECRET 'lakekeeper_secret'
);
ATTACH 'facility_ops' AS facility_ops (
    TYPE iceberg,
    ENDPOINT 'http://localhost:50080/iceberg/catalog',
    SECRET 'lakekeeper_secret'
);
