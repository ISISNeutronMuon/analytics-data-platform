"""Tests for elt_common.iceberg.writer"""

import pyarrow as pa
import pytest
from unittest.mock import MagicMock

from elt_common.iceberg.writer import (
    IcebergIO,
)


@pytest.fixture
def sample_arrow_table():
    return pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
            "ts": pa.array(
                [
                    "2024-01-15T10:00:00",
                    "2024-02-20T11:00:00",
                    "2024-03-25T12:00:00",
                ],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )


def test_ensure_namespace_creates_when_missing():
    from pyiceberg.exceptions import NoSuchNamespaceError

    catalog = MagicMock()
    catalog.load_namespace_properties.side_effect = NoSuchNamespaceError("nope")

    writer = IcebergIO(
        catalog,
    )
    writer.ensure_namespace("test_ns")

    catalog.create_namespace.assert_called_once_with("test_ns")


def test_ensure_namespace_noop_when_exists():
    catalog = MagicMock()
    catalog.load_namespace_properties.return_value = {}

    writer = IcebergIO(catalog)
    writer.ensure_namespace("test_ns")

    catalog.create_namespace.assert_not_called()


def test_write_table_skips_empty_data(sample_arrow_table):
    catalog = MagicMock()
    writer = IcebergIO(catalog)

    empty = sample_arrow_table.slice(0, 0)
    writer.write_table(("ns", "t"), empty, mode="append")

    catalog.load_table.assert_not_called()
    catalog.create_table.assert_not_called()


def test_write_table_append_creates_and_appends(sample_arrow_table):
    catalog = MagicMock()
    mock_table = MagicMock()
    mock_schema = MagicMock()
    mock_schema.column_names = ["id", "name", "ts"]
    mock_table.schema.return_value = mock_schema

    catalog.table_exists.return_value = False
    catalog.create_table.return_value = mock_table

    writer = IcebergIO(catalog)
    writer.write_table(("ns", "t"), sample_arrow_table, mode="append")

    catalog.create_table.assert_called_once()
    mock_table.append.assert_called_once_with(sample_arrow_table)


def test_write_table_merge_requires_merge_on(sample_arrow_table):
    catalog = MagicMock()
    mock_table = MagicMock()
    mock_schema = MagicMock()
    mock_schema.column_names = ["id", "name", "ts"]
    mock_table.schema.return_value = mock_schema

    catalog.table_exists.return_value = True
    catalog.load_table.return_value = mock_table

    writer = IcebergIO(catalog)
    with pytest.raises(ValueError, match="merge_on must be provided"):
        writer.write_table(("ns", "t"), sample_arrow_table, mode="merge")


def test_write_table_merge_calls_upsert(sample_arrow_table):
    catalog = MagicMock()
    mock_table = MagicMock()
    mock_schema = MagicMock()
    mock_schema.column_names = ["id", "name", "ts"]
    mock_table.schema.return_value = mock_schema

    catalog.table_exists.return_value = True
    catalog.load_table.return_value = mock_table

    writer = IcebergIO(catalog)
    writer.write_table(("ns", "t"), sample_arrow_table, mode="merge", merge_on=["id"])

    mock_table.upsert.assert_called_once_with(
        df=sample_arrow_table,
        join_cols=["id"],
        when_matched_update_all=True,
        when_not_matched_insert_all=True,
        case_sensitive=True,
    )


def test_write_table_replace_deletes_then_appends(sample_arrow_table):
    catalog = MagicMock()
    mock_table = MagicMock()
    mock_schema = MagicMock()
    mock_schema.column_names = ["id", "name", "ts"]
    mock_table.schema.return_value = mock_schema

    catalog.table_exists.return_value = True
    catalog.load_table.return_value = mock_table

    writer = IcebergIO(catalog)
    writer.write_table(("ns", "t"), sample_arrow_table, mode="replace")

    mock_table.delete.assert_called_once()
    mock_table.append.assert_called_once_with(sample_arrow_table)


def test_write_table_invalid_mode_raises(sample_arrow_table):
    catalog = MagicMock()
    mock_table = MagicMock()
    mock_schema = MagicMock()
    mock_schema.column_names = ["id", "name", "ts"]
    mock_table.schema.return_value = mock_schema

    catalog.table_exists.return_value = True
    catalog.load_table.return_value = mock_table

    writer = IcebergIO(catalog)
    with pytest.raises(ValueError, match="Unsupported write mode"):
        writer.write_table("t", sample_arrow_table, mode="invalid")  # type: ignore
