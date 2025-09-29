from elt_common.iceberg.maintenance import TrinoCredentials, TrinoQueryEngine
import pytest

from e2e_tests.conftest import Warehouse


@pytest.fixture(scope="session")
def trino_engine(warehouse: Warehouse):
    server = warehouse.server
    server_settings = server.settings
    server_settings = warehouse.server.settings
    creds = TrinoCredentials(
        host=server_settings.trino_host,
        port=server_settings.trino_port,
        user=server_settings.trino_user,
        password=server_settings.trino_password,
        catalog="",
        http_scheme="http",
    )
    # Use one connection to create the catalog
    trino_catalog_creator = TrinoQueryEngine(creds)
    trino_catalog_creator.execute(
        f"""create catalog {warehouse.name} using iceberg
with (
  "iceberg.catalog.type" = 'rest',
  "iceberg.rest-catalog.warehouse" = '{warehouse.name}',
  "iceberg.rest-catalog.uri" = '{server.catalog_endpoint().value(use_internal_netloc=True)}',
  "iceberg.rest-catalog.vended-credentials-enabled" = 'false',
  "iceberg.rest-catalog.security" = 'OAUTH2',
  "iceberg.rest-catalog.oauth2.server-uri" = '{server.token_endpoint.value(use_internal_netloc=True)}',
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
    del trino_catalog_creator

    # Create another connector connected to the new catalog
    creds.catalog = warehouse.name
    trino_engine = TrinoQueryEngine(creds)
    try:
        yield trino_engine
    finally:
        trino_engine.execute(f"drop catalog {warehouse.name}")
