from elt_common.iceberg.maintenance import TrinoCredentials, TrinoQueryEngine
import pytest

from e2e_tests.conftest import Warehouse


@pytest.fixture(scope="session")
def trino_catalog(warehouse: Warehouse):
    trino_engine = TrinoQueryEngine(
        TrinoCredentials(
            host="localhost",
            port="58088",
            user="trino",
            password="",
            catalog="",
            http_scheme="http",
        )
    )
    server = warehouse.server
    server_settings = server.settings
    trino_engine.execute(
        f"""create catalog {warehouse.name} using iceberg
with (
  "iceberg.catalog.type" = 'rest',
  "iceberg.rest-catalog.warehouse" = '{warehouse.name}',
  "iceberg.rest-catalog.uri" = '{server.catalog_endpoint()}',
  "iceberg.rest-catalog.vended-credentials-enabled" = 'false',
  "iceberg.rest-catalog.security" = 'OAUTH2',
  "iceberg.rest-catalog.oauth2.server-uri" = '{server.token_endpoint}',
  "iceberg.rest-catalog.oauth2.credential" = '{server_settings.openid_client_id}:{server.settings.openid_client_secret}',
  "iceberg.rest-catalog.oauth2.scope" = 'lakekeeper offline_access',
  "fs.native-s3.enabled" = 'true',
  "s3.endpoint" = '{server_settings.s3_endpoint}',
  "s3.region" = '{server_settings.s3_region}',
  "s3.path-style-access" = '{str(server_settings.s3_path_style_access).lower()}',
  "s3.aws-access-key" = '{server_settings.s3_access_key}',
  "s3.aws-secret-key" = '{server_settings.s3_secret_key}'
)
"""
    )

    try:
        yield trino_engine
    finally:
        trino_engine.execute(f"drop catalog {warehouse.name}")
