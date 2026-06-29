"""Iceberg catalog configuration.

Reads connection properties from environment variables and provides
a ``connect_catalog()`` helper that returns a connected pyiceberg ``Catalog``.
"""

import logging

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.typedef import Identifier
from pyiceberg.utils.config import Config as IcebergCatalogConfig

LOGGER = logging.getLogger(__name__)


def connect_catalog(warehouse_name: str) -> Catalog:
    """Connect to the 'default' Iceberg catalog.

    Loads configuration as per `pyiceberg`_, except the value of 'warehouse' which is set directly.

    :param warehouse_name: the name of the warehouse to connect to
    :return: a catalog that can be used for reading and writing

    .. _pyiceberg: https://py.iceberg.apache.org/configuration/
    """
    """The default load_catalog only allows environment variables set before the first import or pyiceberg.catalog"""
    config = IcebergCatalogConfig()
    name = config.get_default_catalog_name()
    conf = config.get_catalog_config(name)

    if conf is None:
        raise RuntimeError(f"Couldn't load iceberg configuration for for catalog '{name}'")

    if "warehouse" in conf and warehouse_name != conf["warehouse"]:
        msg = (
            "elt configures the destination warehouse based on the pipeline directory. "
            "Preconfigured value '%s' is being replaced by '%s'"
        )
        LOGGER.warning(msg, conf["warehouse"], warehouse_name)

    conf["warehouse"] = warehouse_name

    return load_catalog(name, **conf)


def table_identifier(namespace: str, table_name: str) -> Identifier:
    """Construct a standard fully-qualified table name."""
    return namespace, table_name
