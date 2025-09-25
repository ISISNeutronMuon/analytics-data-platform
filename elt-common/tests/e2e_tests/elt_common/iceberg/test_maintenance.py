from elt_common.dlt_destinations.pyiceberg.catalog import create_catalog, PyIcebergCatalog
from elt_common.dlt_destinations.pyiceberg.configuration import PyIcebergCatalogCredentials
import pytest

from e2e_tests.conftest import Warehouse


def connect_to_catalog(warehouse: Warehouse) -> PyIcebergCatalog:
    creds = PyIcebergCatalogCredentials()
    creds.uri = warehouse.server.catalog_url
    creds.warehouse = warehouse.name
    creds.oauth2_server_uri = warehouse.server.token_endpoint
    creds.client_id = warehouse.server.openid_client_id
    creds.client_secret = warehouse.server.openid_client_secret
    creds.scope = warehouse.server.openid_scope
    return create_catalog(name="default", **creds.as_dict())


def populate_warehouse_with_tables(warehouse: Warehouse):
    connect_to_catalog(warehouse)


def test_maintenance_runs_on_all_tables_by_default(warehouse: Warehouse):
    populate_warehouse_with_tables(warehouse)
    pytest.fail("Write a succeeding test")
