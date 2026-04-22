"""Tests for elt_common.iceberg.catalog"""

import pytest
from unittest.mock import MagicMock, patch

from elt_common.iceberg.catalog import (
    connect_catalog,
    load_table,
    table_id,
)


class TestConnectCatalog:
    """Tests for connect_catalog function."""

    @patch("elt_common.iceberg.catalog.load_catalog")
    @patch("elt_common.iceberg.catalog.IcebergCatalogConfig")
    def test_connect_catalog_loads_default_catalog(self, mock_config_class, mock_load_catalog):
        """Test that connect_catalog loads the default catalog from config."""
        # Setup
        mock_config = MagicMock()
        mock_config.get_default_catalog_name.return_value = "test_catalog"
        mock_config.get_catalog_config.return_value = {"warehouse": "/tmp/warehouse"}
        mock_config_class.return_value = mock_config

        mock_catalog = MagicMock()
        mock_load_catalog.return_value = mock_catalog

        # Execute
        result = connect_catalog()

        # Assert
        mock_config.get_default_catalog_name.assert_called_once()
        mock_config.get_catalog_config.assert_called_once_with("test_catalog")
        mock_load_catalog.assert_called_once_with("test_catalog", warehouse="/tmp/warehouse")
        assert result == mock_catalog

    @patch("elt_common.iceberg.catalog.load_catalog")
    @patch("elt_common.iceberg.catalog.IcebergCatalogConfig")
    def test_connect_catalog_passes_all_config_options(self, mock_config_class, mock_load_catalog):
        """Test that all config options are passed to load_catalog."""
        # Setup
        mock_config = MagicMock()
        mock_config.get_default_catalog_name.return_value = "my_catalog"
        mock_config.get_catalog_config.return_value = {
            "warehouse": "/data/warehouse",
            "uri": "http://localhost:8181",
            "auth": "oauth2",
        }
        mock_config_class.return_value = mock_config

        # Execute
        connect_catalog()

        # Assert
        mock_load_catalog.assert_called_once_with(
            "my_catalog",
            warehouse="/data/warehouse",
            uri="http://localhost:8181",
            auth="oauth2",
        )


class TestLoadTable:
    """Tests for load_table function."""

    def test_load_table_returns_table_when_exists(self):
        """Test that load_table returns the table when it exists."""
        # Setup
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.catalog.load_table.return_value = mock_table

        # Execute
        result = load_table(mock_catalog, ("namespace", "table_name"))

        # Assert
        assert result == mock_table
        mock_catalog.catalog.load_table.assert_called_once_with(("namespace", "table_name"))

    def test_load_table_returns_none_when_table_not_found(self):
        """Test that load_table returns None when table doesn't exist."""
        from pyiceberg.exceptions import NoSuchTableError

        # Setup
        mock_catalog = MagicMock()
        mock_catalog.catalog.load_table.side_effect = NoSuchTableError("Not found")

        # Execute
        result = load_table(mock_catalog, ("namespace", "table_name"))

        # Assert
        assert result is None

    def test_load_table_propagates_other_exceptions(self):
        """Test that load_table propagates exceptions other than NoSuchTableError."""
        # Setup
        mock_catalog = MagicMock()
        mock_catalog.catalog.load_table.side_effect = RuntimeError("Connection failed")

        # Execute & Assert
        with pytest.raises(RuntimeError, match="Connection failed"):
            load_table(mock_catalog, ("namespace", "table_name"))


class TestTableId:
    """Tests for table_id function."""

    def test_table_id_returns_tuple_identifier(self):
        """Test that table_id returns a tuple with namespace and table name."""
        # Execute
        result = table_id("my_namespace", "my_table")

        # Assert
        assert result == ("my_namespace", "my_table")
        assert isinstance(result, tuple)
        assert len(result) == 2
