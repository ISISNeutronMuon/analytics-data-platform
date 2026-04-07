"""Tests for elt_common.iceberg.watermark"""

import datetime as dt

import pyarrow as pa
import pytest
from unittest.mock import MagicMock

from elt_common.iceberg.watermark import (
    WATERMARK_INGEST_COLUMN_KEY,
    WATERMARK_INGEST_VALUE_KEY,
    create_watermark_property,
    get_watermark_property_value,
)


def _make_iceberg_table(properties: dict, arrow_schema: pa.Schema) -> MagicMock:
    """Return a mock IcebergTable with the given properties and arrow schema."""
    table = MagicMock()
    table.properties = properties
    table.schema.return_value.as_arrow.return_value = arrow_schema
    return table


def test_get_watermark_returns_none_when_no_properties_set():
    table = _make_iceberg_table({}, pa.schema([]))
    assert get_watermark_property_value(table) is None


def test_get_watermark_returns_none_when_column_property_missing():
    table = _make_iceberg_table(
        {WATERMARK_INGEST_VALUE_KEY: "abc"},
        pa.schema([]),
    )
    assert get_watermark_property_value(table) is None


def test_get_watermark_returns_none_when_table_column_missing():
    table = _make_iceberg_table(
        {
            WATERMARK_INGEST_COLUMN_KEY: "expected_column_name",
            WATERMARK_INGEST_VALUE_KEY: "abc",
        },
        pa.schema([pa.field("not_watermark_column", pa.string())]),
    )
    assert get_watermark_property_value(table) is None


def test_get_watermark_returns_none_when_value_property_missing():
    table = _make_iceberg_table(
        {WATERMARK_INGEST_COLUMN_KEY: "updated_at"},
        pa.schema([pa.field("updated_at", pa.string())]),
    )
    assert get_watermark_property_value(table) is None


def test_get_watermark_raises_for_unsupported_column_type():
    schema = pa.schema([pa.field("weight", pa.float64())])
    table = _make_iceberg_table(
        {
            WATERMARK_INGEST_COLUMN_KEY: "weight",
            WATERMARK_INGEST_VALUE_KEY: "5.4",
        },
        schema,
    )
    table.name = "my_table"
    with pytest.raises(ValueError, match="Unsupported column type"):
        get_watermark_property_value(table)


def test_create_watermark_raises_when_table_column_missing():
    table = _make_iceberg_table(
        {
            WATERMARK_INGEST_COLUMN_KEY: "expected_column_name",
            WATERMARK_INGEST_VALUE_KEY: "abc",
        },
        pa.schema([pa.field("not_watermark_column", pa.string())]),
    )
    data = pa.Table.from_pydict({"comment": ["comment 1"]})
    table.name = "my_table"
    with pytest.raises(KeyError):
        create_watermark_property(data, column="id")


# def test_create_and_get_roundtrip_for_timestamp_column():
#     updated_at =
#     data = pa.Table.from_pydict({"updated_at": updated_at})
#     table = _make_iceberg_table(
#         create_watermark_property(data, "updated_at"),
#         data.schema,
#     )

#     assert get_watermark_property_value(table) == updated_at[-1]


# def test_create_and_get_roundtrip_for_integer_column():
#     ids = [index + 1 for index in range(10)]
#     data = pa.Table.from_pydict({"id": ids})
#     table = _make_iceberg_table(
#         create_watermark_property(data, "id"),
#         data.schema,
#     )

#     assert get_watermark_property_value(table) == ids[-1]


@pytest.mark.parametrize(
    ("watermark_column", "column_values"),
    [
        ("id", [index + 1 for index in range(10)]),
        ("updated_at", [dt.datetime(2024, 1, 15, 0, 35, seconds) for seconds in range(10)]),
    ],
)
def test_create_and_get_watermark_roundtrip(watermark_column, column_values):
    data = pa.Table.from_pydict({watermark_column: column_values})
    table = _make_iceberg_table(
        create_watermark_property(data, watermark_column),
        data.schema,
    )

    assert get_watermark_property_value(table) == column_values[-1]


#     def test_get_watermark_returns_datetime_value_for_timestamp_column(self):
#         assert get_watermark_property_value(table) == watermark_value.as_py()


# class TestCreateWatermarkProperty:
#     def test_returns_property_dict_with_max_string_value(self):
#         data = pa.table({"tag": pa.array(["apple", "cherry", "banana"], type=pa.string())})
#         result = create_watermark_property(data, "tag")
#         assert result == {
#             WATERMARK_INGEST_COLUMN_KEY: "tag",
#             WATERMARK_INGEST_VALUE_KEY: "cherry",
#         }

#     def test_returns_property_dict_with_max_integer_value(self):
#         data = pa.table({"run_id": pa.array([10, 3, 77, 42], type=pa.int64())})
#         result = create_watermark_property(data, "run_id")
#         assert result == {
#             WATERMARK_INGEST_COLUMN_KEY: "run_id",
#             WATERMARK_INGEST_VALUE_KEY: "77",
#         }

#     def test_value_stored_as_string(self):
#         data = pa.table({"count": pa.array([1, 2, 3], type=pa.int64())})
#         result = create_watermark_property(data, "count")
#         assert isinstance(result[WATERMARK_INGEST_VALUE_KEY], str)
