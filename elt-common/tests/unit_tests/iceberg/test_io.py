"""Tests for elt_common.iceberg.writer"""

from collections import namedtuple
import datetime as dt

from elt_common.iceberg.io import IcebergIO
from elt_common.extract import Watermark
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.table import ALWAYS_TRUE, Table
import pytest
from unittest.mock import MagicMock


MockedDependencies = namedtuple("MockedDependencies", ["mock_catalog", "mock_transaction"])


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
    mock_catalog = MagicMock(spec=Catalog)
    mock_table = MagicMock(spec=Table)
    mock_transaction = MagicMock()
    mock_table.transaction.return_value.__enter__.return_value = mock_transaction
    mock_catalog.create_table.return_value = mock_table
    mock_catalog.load_table.return_value = mock_table

    return MockedDependencies(
        mock_catalog=mock_catalog,
        mock_transaction=mock_transaction,
    )


def test_ensure_namespace_creates_when_missing(mock_dependencies: MockedDependencies):
    """Tests for IcebergIO.ensure_namespace creates namespace when missing"""
    mock_dependencies.mock_catalog.namespace_exists.return_value = False
    io = IcebergIO(mock_dependencies.mock_catalog)

    io.ensure_namespace("test_ns")
    mock_dependencies.mock_catalog.create_namespace.assert_called_once_with("test_ns")


def test_ensure_namespace_noop_when_namespace_exists(mock_dependencies: MockedDependencies):
    """Tests for IcebergIO.ensure_namespace when namespace exists"""
    mock_dependencies.mock_catalog.namespace_exists.return_value = True
    io = IcebergIO(mock_dependencies.mock_catalog)

    io.ensure_namespace("test_ns")
    mock_dependencies.mock_catalog.assert_not_called()


def test_read_property_loads_table_and_returns_property_if_exists(
    mock_dependencies: MockedDependencies,
):
    mock_dependencies.mock_catalog.load_table.return_value.properties = {"test.key": "value"}
    io = IcebergIO(mock_dependencies.mock_catalog)

    result = io.read_property(("ns", "t"), "test.key")

    assert result == "value"
    mock_dependencies.mock_catalog.load_table.assert_called_once_with(("ns", "t"))


def test_read_property_raises_KeyError_property_if_missing(
    mock_dependencies: MockedDependencies,
):
    mock_dependencies.mock_catalog.load_table.return_value.properties = {}
    io = IcebergIO(mock_dependencies.mock_catalog)

    with pytest.raises(KeyError):
        io.read_property(("ns", "t"), "test.key")

    mock_dependencies.mock_catalog.load_table.assert_called_once_with(("ns", "t"))


def test_write_table_skips_empty_data(
    mock_dependencies: MockedDependencies, sample_arrow_table: pa.Table
):
    """Tests for IcebergIO.write_table skips empty data"""
    io = IcebergIO(mock_dependencies.mock_catalog)

    empty = sample_arrow_table.slice(0, 0)
    io.write_table(("ns", "t"), empty, "append")

    mock_dependencies.mock_catalog.load_table.assert_not_called()
    mock_dependencies.mock_catalog.create_table.assert_not_called()


def test_write_table_append_creates_and_appends(
    mock_dependencies: MockedDependencies, sample_arrow_table: pa.Table
):
    """Tests for IcebergIO.write_table append mode"""
    mock_catalog = mock_dependencies.mock_catalog
    mock_catalog.table_exists.return_value = False

    io = IcebergIO(mock_catalog)
    io.write_table(("ns", "t"), sample_arrow_table, "append")

    mock_dependencies.mock_catalog.load_table.assert_not_called()
    mock_dependencies.mock_catalog.create_table.assert_called_once()
    mock_dependencies.mock_transaction.append.assert_called_once_with(sample_arrow_table)


def test_write_table_merge_requires_merge_on(
    mock_dependencies: MockedDependencies, sample_arrow_table
):
    """Tests for IcebergIO.write_table merge mode requires merge_on"""
    mock_dependencies.mock_catalog.table_exists.return_value = True
    io = IcebergIO(mock_dependencies.mock_catalog)

    with pytest.raises(ValueError, match=r".*write mode 'merge' requires 'merge_on' property\."):
        io.write_table(("ns", "t"), sample_arrow_table, "merge")


def test_write_table_merge_calls_upsert(mock_dependencies: MockedDependencies, sample_arrow_table):
    """Tests for IcebergIO.write_table merge mode calls upsert"""
    mock_dependencies.mock_catalog.table_exists.return_value = True

    io = IcebergIO(mock_dependencies.mock_catalog)
    io.write_table(
        ("ns", "t"),
        sample_arrow_table,
        "merge",
        merge_on=["id"],
    )

    mock_dependencies.mock_transaction.upsert.assert_called_once_with(
        df=sample_arrow_table,
        join_cols=["id"],
        when_matched_update_all=True,
        when_not_matched_insert_all=True,
        case_sensitive=True,
    )


def test_write_table_sets_watermark_properties_if_supplied(
    mock_dependencies: MockedDependencies, sample_arrow_table: pa.Table
):
    mock_dependencies.mock_catalog.table_exists.return_value = True
    watermark = "2024-01-15T10:00:00Z"

    io = IcebergIO(mock_dependencies.mock_catalog)
    io.write_table(
        ("ns", "t"),
        sample_arrow_table,
        "append",
        watermark=watermark,
    )

    mock_dependencies.mock_transaction.append.assert_called_once_with(sample_arrow_table)
    mock_dependencies.mock_transaction.set_properties.assert_called_once_with(
        {Watermark.property_key(): "2024-01-15T10:00:00Z"}
    )


def test_write_table_calls_overwrite(
    mock_dependencies: MockedDependencies, sample_arrow_table: pa.Table
):
    """Tests for IcebergIO.write_table replace mode"""
    mock_catalog = mock_dependencies.mock_catalog
    mock_catalog.table_exists.return_value = True

    io = IcebergIO(mock_catalog)
    io.write_table(
        ("ns", "t"),
        sample_arrow_table,
        "replace",
    )

    mock_dependencies.mock_transaction.overwrite.assert_called_once_with(
        sample_arrow_table, overwrite_filter=ALWAYS_TRUE, case_sensitive=True
    )
