"""Test helpers for Iceberg catalog-based pipelines (replaces testing/dlt.py)."""

import os
import time
from collections.abc import MutableMapping
from dataclasses import dataclass

from pyiceberg.catalog import Catalog
from elt_common.iceberg.writer import IcebergWriter

from .lakekeeper import RestCatalogWarehouse
from .sqlcatalog import SqlCatalogWarehouse


@dataclass
class IcebergTestConfiguration:
    """Test configuration that sets environment variables for ``CatalogConfig``
    and provides helpers for connecting to a test catalog.
    """

    warehouse: SqlCatalogWarehouse | RestCatalogWarehouse

    def setup(self, environ: MutableMapping = os.environ) -> None:
        """Populate environment variables so ``CatalogConfig()`` picks them up."""
        if isinstance(self.warehouse, SqlCatalogWarehouse):
            environ["DESTINATION__PYICEBERG__CATALOG_TYPE"] = "sql"
            environ["DESTINATION__PYICEBERG__CREDENTIALS__URI"] = self.warehouse.uri
            environ["DESTINATION__PYICEBERG__CREDENTIALS__WAREHOUSE"] = (
                self.warehouse.warehouse_path
            )
        else:
            server, settings = self.warehouse.server, self.warehouse.server.settings
            environ["DESTINATION__PYICEBERG__CATALOG_TYPE"] = "rest"
            environ["DESTINATION__PYICEBERG__CREDENTIALS__URI"] = str(
                self.warehouse.server.catalog_endpoint()
            )
            environ.setdefault(
                "DESTINATION__PYICEBERG__CREDENTIALS__WAREHOUSE", self.warehouse.name
            )
            environ.setdefault(
                "DESTINATION__PYICEBERG__CREDENTIALS__OAUTH2_SERVER_URI",
                str(server.token_endpoint),
            )
            environ.setdefault(
                "DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_ID",
                settings.openid_client_id,
            )
            environ.setdefault(
                "DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_SECRET",
                settings.openid_client_secret,
            )
            environ.setdefault(
                "DESTINATION__PYICEBERG__CREDENTIALS__SCOPE",
                settings.openid_scope,
            )

    def connect(self) -> Catalog:
        """Connect to the test catalog."""
        return self.warehouse.connect()

    def writer(self, namespace: str) -> IcebergWriter:
        """Create an ``IcebergWriter`` for the given namespace."""
        return IcebergWriter(self.connect(), namespace)

    def clean_catalog(self) -> None:
        """Purge all namespaces and tables from the catalog."""
        catalog = self.connect()
        for ns_name in catalog.list_namespaces():
            for qualified_table_name in catalog.list_tables(ns_name):
                catalog.purge_table(qualified_table_name)
            catalog.drop_namespace(ns_name)
        time.sleep(0.1)
