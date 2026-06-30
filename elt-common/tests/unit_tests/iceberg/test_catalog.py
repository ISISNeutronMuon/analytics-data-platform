"""Tests for elt_common.iceberg.catalog"""

from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from elt_common.iceberg.catalog import (
    connect_catalog,
    table_identifier,
)


@pytest.fixture
def mock_config(mocker: MockerFixture):
    mock_config_cls = mocker.patch("elt_common.iceberg.catalog.IcebergCatalogConfig")
    mock_config = MagicMock()
    mock_config.get_default_catalog_name.return_value = "default"
    mock_config.get_catalog_config.return_value = {"warehouse": "/tmp/warehouse"}
    mock_config_cls.return_value = mock_config

    return mock_config


@pytest.fixture
def mock_load_catalog(mocker: MockerFixture):
    return mocker.patch("elt_common.iceberg.catalog.load_catalog")


def test_no_config_found_raises_error(mock_config):
    mock_config.get_catalog_config.return_value = None
    with pytest.raises(RuntimeError):
        connect_catalog("test_warehouse")


def test_connect_catalog_loads_default_catalog(mock_config, mock_load_catalog):
    # Execute
    connect_catalog("test_warehouse")

    # Assert
    mock_config.get_default_catalog_name.assert_called_once()
    mock_config.get_catalog_config.assert_called_once_with("default")
    mock_load_catalog.assert_called_once_with("default", warehouse="test_warehouse")


def test_connect_catalog_forwards_all_options_from_pyiceberg_catalog_config(
    mock_config, mock_load_catalog
):
    catalog_config = {
        "warehouse": "/data/warehouse",
        "uri": "http://localhost:8181",
        "auth": "oauth2",
    }
    # 'warehouse' is overwritten by the provided value
    expected_config = {k: v for k, v in catalog_config.items()}
    expected_config["warehouse"] = "test_warehouse"

    mock_config.get_catalog_config.return_value = catalog_config

    # Execute
    connect_catalog("test_warehouse")

    # Assert
    mock_load_catalog.assert_called_once_with("default", **expected_config)


def test_table_id_returns_tuple_identifier():
    """Test that table_id returns a tuple with namespace and table name."""
    result = table_identifier("my_namespace", "my_table")
    assert result == ("my_namespace", "my_table")
