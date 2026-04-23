"""Tests for elt_common.iceberg.catalog"""

import pytest
from pytest_mock import MockerFixture
from unittest.mock import MagicMock

from elt_common.iceberg.catalog import (
    connect_catalog,
    table_id,
)


@pytest.fixture
def mock_pyiceberg(mocker: MockerFixture):
    mock_config_cls = mocker.patch("elt_common.iceberg.catalog.IcebergCatalogConfig")
    mock_config = MagicMock()
    mock_config.get_default_catalog_name.return_value = "default"
    mock_config.get_catalog_config.return_value = {"warehouse": "/tmp/warehouse"}
    mock_config_cls.return_value = mock_config

    mock_load_catalog = mocker.patch("elt_common.iceberg.catalog.load_catalog")
    return mock_config, mock_load_catalog


def test_connect_catalog_loads_default_catalog(mock_pyiceberg):
    mock_config, mock_load_catalog = mock_pyiceberg[0], mock_pyiceberg[1]

    # Execute
    connect_catalog()

    # Assert
    mock_config.get_default_catalog_name.assert_called_once()
    mock_config.get_catalog_config.assert_called_once_with("default")
    mock_load_catalog.assert_called_once_with("default", warehouse="/tmp/warehouse")


def test_connect_catalog_forwards_all_options_from_pyiceberg_catalog_config(mock_pyiceberg):
    mock_config, mock_load_catalog = mock_pyiceberg[0], mock_pyiceberg[1]
    catalog_config = {
        "warehouse": "/data/warehouse",
        "uri": "http://localhost:8181",
        "auth": "oauth2",
    }
    mock_config.get_catalog_config.return_value = catalog_config

    # Execute
    connect_catalog()

    # Assert
    mock_load_catalog.assert_called_once_with("default", **catalog_config)


def test_table_id_returns_tuple_identifier():
    """Test that table_id returns a tuple with namespace and table name."""
    # Execute
    result = table_id("my_namespace", "my_table")

    # Assert
    assert result == ("my_namespace", "my_table")
    assert isinstance(result, tuple)
    assert len(result) == 2
