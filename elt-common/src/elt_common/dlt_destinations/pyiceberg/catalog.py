from typing import Union
from pyiceberg.catalog.rest import (
    Catalog as PyIcebergCatalog,
)
from pyiceberg.typedef import Identifier
from pyiceberg.exceptions import NoSuchNamespaceError as PyIcebergNoSuchNamespaceError


def create_catalog(name: str, **properties: str) -> PyIcebergCatalog:
    """Create an Iceberg catalog

    Args:
        name: Name to identify the catalog.
        properties: Properties that are passed along to the configuration.
    """

    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(name, **properties)


def namespace_exists(catalog: PyIcebergCatalog, namespace: Union[str, Identifier]) -> bool:
    try:
        catalog.load_namespace_properties(namespace)
        return True
    except PyIcebergNoSuchNamespaceError:
        return False
