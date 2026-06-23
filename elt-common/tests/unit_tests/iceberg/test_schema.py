"""Tests for elt_common.iceberg.schema"""

import pyarrow as pa
import pytest
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
)

from elt_common.iceberg.schema import arrow_type_to_iceberg, create_schema, evolve_schema


arrow_fields = [
    pa.field("row_id", pa.int64(), nullable=False),
    pa.field("entry_name", pa.string(), nullable=False),
    pa.field("entry_timestamp", pa.timestamp(unit="us")),
    pa.field("entry_weight", pa.float64()),
]


@pytest.fixture()
def arrow_schema() -> pa.Schema:
    return pa.schema(arrow_fields)


iceberg_fields = [
    NestedField(field_id=1, name="row_id", field_type=LongType(), required=True),
    NestedField(field_id=2, name="entry_name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="entry_timestamp", field_type=TimestampType()),
    NestedField(field_id=4, name="entry_weight", field_type=DoubleType()),
]


def test_unsupported_arrow_type_raises():
    with pytest.raises(TypeError, match="unknown to type mapper"):
        arrow_type_to_iceberg(pa.string_view())


@pytest.mark.parametrize(
    ("arrow_type", "expected_type"),
    [
        (pa.bool_(), BooleanType),
        (pa.int32(), IntegerType),
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
        (pa.struct([("test", pa.int32())]), StructType),
        (pa.struct([("nested", pa.struct([("test", pa.int32())]))]), StructType),
        (pa.list_(pa.int32()), ListType),
        (
            pa.list_(
                pa.struct(
                    [
                        ("list_of_structs", pa.list_(pa.struct([("a", pa.int32())]))),
                        ("something", pa.binary()),
                    ]
                )
            ),
            ListType,
        ),
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


@pytest.mark.parametrize("time_type", [pa.time32("s"), pa.time32("ms"), pa.time64("ns")])
def test_time_precision_other_than_microseconds_raises(time_type):
    with pytest.raises(TypeError, match="only supports 'us' precision"):
        arrow_type_to_iceberg(time_type)


def test_timestamp_nanoseconds_raises():
    with pytest.raises(TypeError, match="does not support timestamps"):
        arrow_type_to_iceberg(pa.timestamp("ns"))


def test_create_empty_schema():
    empty_schema = pa.schema([])
    iceberg_schema = create_schema(empty_schema)

    assert len(iceberg_schema.fields) == 0


@pytest.mark.parametrize("identifier_fields", [(), ["row_id", "entry_name"]])
def test_create_iceberg_schema(arrow_schema: pa.Schema, identifier_fields):
    iceberg_schema = create_schema(arrow_schema, identifier_fields)

    assert len(iceberg_schema.fields) == len(arrow_schema.names)
    assert [f.name for f in iceberg_schema.fields] == arrow_schema.names
    assert [not f.required for f in iceberg_schema.fields] == [f.nullable for f in arrow_schema]

    # assume the types are correct as the type mapping is tested above
    if identifier_fields:
        assert iceberg_schema.identifier_field_ids == [1, 2]


@pytest.mark.parametrize(
    ["iceberg_field_idxs", "expected_new_field_names"],
    [
        ([], {"row_id", "entry_name", "entry_timestamp", "entry_weight"}),
        (
            [0, 1, 2],
            {"row_id", "entry_name", "entry_timestamp", "entry_weight"},
        ),
        ([0, 1, 2, 3], {}),
    ],
)
def test_evolve_schema(
    arrow_schema: pa.Schema, iceberg_field_idxs: list[int], expected_new_field_names
):
    existing_fields = [iceberg_fields[i] for i in iceberg_field_idxs]
    existing_schema = Schema(*existing_fields)

    schema_with_new_fields = evolve_schema(existing_schema, arrow_schema)

    if expected_new_field_names:
        assert schema_with_new_fields is not None
        assert {f.name for f in schema_with_new_fields.fields} == expected_new_field_names
    else:
        assert schema_with_new_fields is None


@pytest.mark.parametrize(
    ["iceberg_field_idxs", "new_fields"],
    [
        # Fields removed
        ([0], []),
        ([0], arrow_fields[1:]),
        ([0, 1], arrow_fields[:1]),
        ([0, 1, 2], arrow_fields[:2]),
        ([0], [arrow_fields[1]]),
        ([1, 2], arrow_fields[2:4]),
        # Fields reordered
        ([0, 1], [arrow_fields[1], arrow_fields[0]]),
        ([3, 2, 1], [arrow_fields[1], arrow_fields[3], arrow_fields[2]]),
        # Field property changed
        ([0], [pa.field("row_id_renamed", pa.int64(), nullable=False)]),
        ([0], [pa.field("row_id", pa.int32(), nullable=False)]),
        ([0], [pa.field("row_id", pa.int64(), nullable=True)]),
    ],
)
def test_evolve_schema_incompatible(iceberg_field_idxs, new_fields):
    existing_fields = [iceberg_fields[i] for i in iceberg_field_idxs]
    existing_schema = Schema(*existing_fields)

    new_schema = pa.schema(new_fields)

    with pytest.raises(ValueError):
        evolve_schema(existing_schema, new_schema)
