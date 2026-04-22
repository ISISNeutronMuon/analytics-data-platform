"""Tests for elt_common.iceberg.schema"""

import pyarrow as pa
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    TimeType,
    TimestampType,
    TimestamptzType,
)
import pytest

from elt_common.iceberg.schema import arrow_type_to_iceberg, create_schema


@pytest.fixture()
def arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("row_id", pa.int64(), nullable=False),
            pa.field("entry_name", pa.string(), nullable=False),
            pa.field("entry_timestamp", pa.timestamp(unit="us")),
            pa.field("entry_weight", pa.float64()),
        ]
    )


def test_unsupported_arrow_type_raises():
    with pytest.raises(TypeError, match="unknown to type mapper"):
        arrow_type_to_iceberg(pa.int32())


def test_invalid_arrow_type_string_raises():
    with pytest.raises(TypeError, match="Unknown pyarrow type"):
        arrow_type_to_iceberg(pa.type_for_alias("not-a-real-arrow-type"))  # type: ignore


@pytest.mark.parametrize(
    ("arrow_type", "expected_type"),
    [
        (pa.bool_(), BooleanType),
        (pa.int64(), LongType),
        (pa.float64(), DoubleType),
        (pa.decimal128(20, 5), DecimalType),
        (pa.string(), StringType),
        (pa.large_string(), StringType),
        (pa.date32(), DateType),
        (pa.time64("us"), TimeType),
        (pa.timestamp("us"), TimestampType),
        (pa.timestamp("ms", tz="UTC"), TimestamptzType),
        (pa.binary(), BinaryType),
        (pa.large_binary(), BinaryType),
        (pa.binary(8), BinaryType),
        ("int64", LongType),
        ("string", StringType),
    ],
)
def test_returns_expected_iceberg_type(arrow_type, expected_type):
    result = arrow_type_to_iceberg(arrow_type)
    assert isinstance(result, expected_type)


def test_maps_decimal_precision_and_scale():
    result = arrow_type_to_iceberg(pa.decimal128(12, 3))

    assert isinstance(result, DecimalType)
    assert result.precision == 12
    assert result.scale == 3


def test_time_precision_other_than_microseconds_raises():
    with pytest.raises(TypeError, match="only supports 'us' precision"):
        arrow_type_to_iceberg(pa.time32("ms"))


def test_timestamp_nanoseconds_raises():
    with pytest.raises(TypeError, match="does not support timestamps"):
        arrow_type_to_iceberg(pa.timestamp("ns"))


@pytest.mark.parametrize(("identifier_fields",), [[None], [["row_id", "entry_name"]]])
def test_create_iceberg_schema(arrow_schema: pa.Schema, identifier_fields):
    iceberg_schema = create_schema(arrow_schema, identifier_fields)

    assert len(iceberg_schema.fields) == len(arrow_schema.names)
    assert [f.name for f in iceberg_schema.fields] == arrow_schema.names
    assert [not f.required for f in iceberg_schema.fields] == [f.nullable for f in arrow_schema]
    # assume the types are correct as the type mapping is tested above
    if identifier_fields is not None:
        assert iceberg_schema.identifier_field_ids == [1, 2]
