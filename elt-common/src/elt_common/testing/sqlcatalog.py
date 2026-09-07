from pathlib import Path

from pyiceberg.catalog import Catalog as PyIcebergCatalog
from pyiceberg.catalog import load_catalog

from elt_common.dlt_destinations.pyiceberg.configuration import PyIcebergSqlCatalogCredentials


class SqlCatalogWarehouse:
    def __init__(self, warehouse_name: str, workdir: Path):
        self.name = warehouse_name
        self.workdir = workdir
        self.uri = f"sqlite:///{self.workdir}/{warehouse_name}.db"
        self.warehouse_path = f"file://{self.workdir}/{warehouse_name}"

    def connect(self) -> PyIcebergCatalog:
        """Connect to the warehouse in the catalog"""
        creds = PyIcebergSqlCatalogCredentials()
        creds.uri = self.uri
        creds.warehouse = self.warehouse_path
        return load_catalog(name="default", **creds.as_dict())
