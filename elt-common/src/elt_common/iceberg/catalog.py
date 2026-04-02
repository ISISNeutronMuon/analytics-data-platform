"""Iceberg catalog configuration.

Reads connection properties from environment variables and provides
a ``connect_catalog()`` helper that returns a connected pyiceberg ``Catalog``.
"""

from typing import Any

import pyarrow.compute as pc
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.utils.config import Config as IcebergCatalogConfig
from pyiceberg.typedef import Identifier


def connect_catalog() -> Catalog:
    """The default load_catalog only allows environment variables set before the first import or pyiceberg.catalog"""
    config = IcebergCatalogConfig()
    name = config.get_default_catalog_name()
    return load_catalog(name, **config.get_catalog_config(name))  # type: ignore


def get_max_value(catalog: Catalog, table_id: Identifier, column: str | None) -> Any:
    """Find the maximum value of the column in the table"""
    if column is None or not catalog.table_exists(table_id):
        return None

    table = catalog.load_table(table_id)
    if table is None:
        return None

    # table.properties.get(f"ingest.cursor.{column}.value")

    # Scan in batches to avoid loading large tables into memory all at once
    scan = table.scan(selected_fields=(column,))
    running_max = None
    for batch in scan.to_arrow_batch_reader():
        batch_max = pc.max(batch[column])
        if batch_max.is_valid:
            batch_max = batch_max.as_py()
            running_max = batch_max if running_max is None else max(batch_max, running_max)

    return running_max if running_max is not None else None
