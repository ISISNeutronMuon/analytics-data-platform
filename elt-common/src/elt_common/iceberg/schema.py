from typing import Dict, Sequence, cast

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    NestedField,
    PrimitiveType,
    StringType,
    TimeType,
    TimestampType,
    TimestamptzType,
)
from pyiceberg.typedef import Identifier
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError


TIMESTAMP_PRECISION_TO_UNIT: Dict[int, str] = {0: "s", 3: "ms", 6: "us", 9: "ns"}
UNIT_TO_TIMESTAMP_PRECISION: Dict[str, int] = {v: k for k, v in TIMESTAMP_PRECISION_TO_UNIT.items()}


def arrow_type_to_iceberg(arrow_type: pa.DataType | str) -> PrimitiveType:
    """Returns the Iceberg type for the given pyarrow data type.

    :raises TypeError: If the type is unknown or is not supported
    """
    if isinstance(arrow_type, str):
        try:
            arrow_type = pa.type_for_alias(arrow_type)
        except (TypeError, ValueError):
            raise TypeError(f"Unknown pyarrow type '{arrow_type}'.")

    if pa.types.is_boolean(arrow_type):
        return BooleanType()
    elif pa.types.is_int64(arrow_type):
        return LongType()
    elif pa.types.is_float64(arrow_type):
        return DoubleType()
    elif pa.types.is_decimal(arrow_type):
        decimal_type = cast(DecimalType, arrow_type)
        return DecimalType(decimal_type.precision, decimal_type.scale)
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return StringType()
    elif pa.types.is_date(arrow_type):
        return DateType()
    elif pa.types.is_time(arrow_type):
        time_type = cast(TimeType, arrow_type)
        if time_type.unit != "us":
            raise TypeError(
                f"Iceberg time type only supports 'us' precision. Requested precision={arrow_type.unit}'."
            )
        return TimeType()
    elif pa.types.is_timestamp(arrow_type):
        if arrow_type.unit == "ns":
            raise TypeError(
                f"Iceberg v1 & v2 does not support timestamps in '{TIMESTAMP_PRECISION_TO_UNIT[9]}' precision."
            )
        if arrow_type.tz is not None:
            return TimestamptzType()
        else:
            return TimestampType()
    elif (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
    ):
        return BinaryType()
    else:
        raise TypeError(f"Pyarrow type '{arrow_type}' unknown to type mapper.")


def create_iceberg_schema(
    arrow_schema: pa.Schema, identifier_fields: Sequence[str] | None = None
) -> Schema:
    """Create a Iceberg schema based on a dlt schema

    :param arrow_schema: An existing arrow_schema.
    :param primary_keys: An optional list of fields to mark as identifiers
    """
    if identifier_fields is None:
        identifier_fields = ()
    iceberg_fields, identifier_field_ids = [], []
    for index, arrow_field in enumerate(arrow_schema):
        col_id = index + 1
        iceberg_fields.append(
            NestedField(
                col_id,
                arrow_field.name,
                arrow_type_to_iceberg(arrow_field.type),
                required=not arrow_field.nullable,
            )
        )
        if arrow_field.name in identifier_fields:
            identifier_field_ids.append(col_id)

    return Schema(*iceberg_fields, identifier_field_ids=identifier_field_ids)
