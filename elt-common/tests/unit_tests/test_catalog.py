"""Tests for elt_common.catalog"""

from unittest.mock import MagicMock, patch

from elt_common.iceberg.catalog import connect_catalog


def test_connect_catalog_calls_load_catalog_with_config():
    """connect_catalog reads name and properties from IcebergCatalogConfig and passes them to load_catalog."""
    mock_config = MagicMock()
    mock_config.get_default_catalog_name.return_value = "default"
    props = {
        "type": "rest",
        "uri": "http://catalog:8181",
        "warehouse": "file:///tmp/wh",
    }
    mock_config.get_catalog_config.return_value = props
    mock_catalog = MagicMock()

    with (
        patch("elt_common.iceberg.catalog.IcebergCatalogConfig", return_value=mock_config),
        patch("elt_common.iceberg.catalog.load_catalog", return_value=mock_catalog) as mock_load,
    ):
        result = connect_catalog()

    mock_config.get_default_catalog_name.assert_called_once()
    mock_config.get_catalog_config.assert_called_once_with("default")
    mock_load.assert_called_once_with("default", **props)
    assert result is mock_catalog
