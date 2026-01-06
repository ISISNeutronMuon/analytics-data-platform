-- Options taken from https://github.com/lakekeeper/console-components/blob/main/src/composables/useIcebergDuckDB.ts

INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;

CREATE SECRET infra_local_iceberg (
    TYPE ICEBERG,
    CLIENT_ID 'machine-infra',
    CLIENT_SECRET 's3cr3t',
    OAUTH2_SCOPE 'lakekeeper',
    OAUTH2_SERVER_URI 'http://localhost:50080/auth/realms/analytics-data-platform/protocol/openid-connect/token'
);

ATTACH 'playground' AS lakekeeper (
    TYPE ICEBERG,
    SUPPORT_NESTED_NAMESPACES true,
    SUPPORT_STAGE_CREATE true,
    ENDPOINT 'http://localhost:50080/iceberg/catalog',
    SECRET infra_local_iceberg
);
