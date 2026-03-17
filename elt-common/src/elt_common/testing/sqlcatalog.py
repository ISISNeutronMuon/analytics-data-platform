from tempfile import TemporaryDirectory

from pyiceberg.catalog import Catalog as PyIcebergCatalog
from pyiceberg.catalog import load_catalog


class SqlCatalogWarehouse:
    def __init__(self, warehouse_name: str):
        self.name = warehouse_name
        self.workdir = TemporaryDirectory()
        self.uri = f"sqlite:///{self.workdir.name}/{warehouse_name}.db"
        self.warehouse_path = f"file://{self.workdir.name}/{warehouse_name}"

    def connect(self) -> PyIcebergCatalog:
        """Connect to the warehouse in the catalog"""
        return load_catalog(
            name="default",
            **{"type": "sql", "uri": self.uri, "warehouse": self.warehouse_path},
        )
