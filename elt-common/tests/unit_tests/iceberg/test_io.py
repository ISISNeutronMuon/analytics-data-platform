"""Tests for elt_common.iceberg.writer"""

from collections import namedtuple
import datetime as dt

from elt_common.iceberg.io import (
    IcebergIO,
)
import pyarrow as pa
import pytest
from unittest.mock import MagicMock

MockedDependencies = namedtuple("MockedDependencies", ["mock_catalog", "mock_table"])


@pytest.fixture(scope="session")
def sample_arrow_table():
    return pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
            "ts": pa.array(
                [
                    dt.datetime.fromisoformat("2024-01-15T10:00:00"),
                    dt.datetime.fromisoformat("2024-02-20T11:00:00"),
                    dt.datetime.fromisoformat("2024-03-25T12:00:00"),
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )


@pytest.fixture
def mock_dependencies() -> MockedDependencies:
    catalog = MagicMock()
    mock_table = MagicMock()
    mock_schema = MagicMock()
    mock_schema.column_names = ["id", "name", "ts"]
    mock_table.schema.return_value = mock_schema

    return MockedDependencies(mock_catalog=catalog, mock_table=mock_table)


class TestIcebergIO:
    """Tests for IcebergIO class"""

    def test_ensure_namespace_creates_when_missing(self, mock_dependencies: MockedDependencies):
        mock_dependencies.mock_catalog.namespace_exists.return_value = False
        writer = IcebergIO(mock_dependencies.mock_catalog)

        writer.ensure_namespace("test_ns")
        mock_dependencies.mock_catalog.create_namespace.assert_called_once_with("test_ns")

    def test_ensure_namespace_noop_when_namespace_exists(
        self, mock_dependencies: MockedDependencies
    ):
        writer = IcebergIO(mock_dependencies.mock_catalog)

        writer.ensure_namespace("test_ns")
        mock_dependencies.mock_catalog.assert_not_called()

    def test_write_table_skips_empty_data(
        self, mock_dependencies: MockedDependencies, sample_arrow_table: pa.Table
    ):
        writer = IcebergIO(mock_dependencies.mock_catalog)

        empty = sample_arrow_table.slice(0, 0)
        writer.write_table(("ns", "t"), empty, mode="append")

        mock_dependencies.mock_catalog.load_table.assert_not_called()
        mock_dependencies.mock_catalog.create_table.assert_not_called()

    def test_write_table_append_creates_and_appends(
        self, mock_dependencies: MockedDependencies, sample_arrow_table: pa.Table
    ):
        mock_catalog = mock_dependencies.mock_catalog
        mock_catalog.table_exists.return_value = False
        mock_catalog.create_table.return_value = mock_dependencies.mock_table

        writer = IcebergIO(mock_catalog)
        writer.write_table(("ns", "t"), sample_arrow_table, mode="append")

        mock_dependencies.mock_catalog.create_table.assert_called_once()
        mock_dependencies.mock_table.append.assert_called_once_with(sample_arrow_table)

    def test_write_table_merge_requires_merge_on(
        self, mock_dependencies: MockedDependencies, sample_arrow_table
    ):
        mock_catalog = mock_dependencies.mock_catalog
        mock_catalog.table_exists.return_value = True
        mock_catalog.load_table.return_value = mock_dependencies.mock_table

        writer = IcebergIO(mock_dependencies.mock_catalog)

        with pytest.raises(ValueError, match="merge_on.*must be provided"):
            writer.write_table(("ns", "t"), sample_arrow_table, mode="merge")

    def test_write_table_merge_calls_upsert(
        self, mock_dependencies: MockedDependencies, sample_arrow_table
    ):
        mock_catalog = mock_dependencies.mock_catalog
        mock_catalog.table_exists.return_value = True
        mock_catalog.load_table.return_value = mock_dependencies.mock_table

        writer = IcebergIO(mock_dependencies.mock_catalog)
        writer.write_table(("ns", "t"), sample_arrow_table, mode="merge", merge_on=["id"])

        mock_dependencies.mock_table.upsert.assert_called_once_with(
            df=sample_arrow_table,
            join_cols=["id"],
            when_matched_update_all=True,
            when_not_matched_insert_all=True,
            case_sensitive=True,
        )

    def test_write_table_replace_deletes_then_appends(
        self, mock_dependencies: MockedDependencies, sample_arrow_table: pa.Table
    ):
        mock_catalog, mock_table = mock_dependencies.mock_catalog, mock_dependencies.mock_table
        mock_catalog.table_exists.return_value = True
        mock_catalog.load_table.return_value = mock_table
        # Mock the transaction context manager
        mock_txn = MagicMock()
        mock_table.transaction.return_value.__enter__ = MagicMock(return_value=mock_txn)
        mock_table.transaction.return_value.__exit__ = MagicMock(return_value=None)

        writer = IcebergIO(mock_catalog)
        writer.write_table(("ns", "t"), sample_arrow_table, mode="replace")

        mock_txn.delete.assert_called_once()
        mock_txn.append.assert_called_once_with(sample_arrow_table)

    def test_write_table_invalid_mode_raises(
        self, mock_dependencies: MockedDependencies, sample_arrow_table: pa.Table
    ):
        writer = IcebergIO(mock_dependencies.mock_catalog)

        with pytest.raises(ValueError, match="Unsupported write mode"):
            writer.write_table("t", sample_arrow_table, mode="invalid")  # type: ignore
