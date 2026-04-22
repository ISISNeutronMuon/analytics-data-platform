"""Iceberg catalog configuration.

Reads connection properties from environment variables and provides
a ``connect_catalog()`` helper that returns a connected pyiceberg ``Catalog``.
"""

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier
from pyiceberg.utils.config import Config as IcebergCatalogConfig


def connect_catalog() -> Catalog:
    """The default load_catalog only allows environment variables set before the first import or pyiceberg.catalog"""
    config = IcebergCatalogConfig()
    name = config.get_default_catalog_name()
    return load_catalog(name, **config.get_catalog_config(name))  # type: ignore


def load_table(self, table_id: Identifier) -> Table | None:
    """Load an existing Iceberg table, or ``None`` if it doesn't exist."""
    try:
        return self.catalog.load_table(table_id)
    except NoSuchTableError:
        return None


def table_id(namespace, table_name) -> Identifier:
    """Construct a standard fully-qualified table name."""
    return (namespace, table_name)
