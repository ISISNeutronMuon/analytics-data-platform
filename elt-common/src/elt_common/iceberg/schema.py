import itertools
from typing import Sequence

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    PrimitiveType,
    StringType,
    TimeType,
    TimestampType,
    TimestamptzType,
)


def arrow_type_to_iceberg(arrow_type: pa.DataType) -> PrimitiveType:
    """Returns the Iceberg type for the given pyarrow data type.

    :raises TypeError: If the type is unknown or is not supported
    """
    if pa.types.is_boolean(arrow_type):
        return BooleanType()
    elif pa.types.is_int32(arrow_type):
        return IntegerType()
    elif pa.types.is_int64(arrow_type):
        return LongType()
    elif pa.types.is_float64(arrow_type):
        return DoubleType()
    elif pa.types.is_decimal(arrow_type):
        return DecimalType(arrow_type.precision, arrow_type.scale)
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return StringType()
    elif pa.types.is_date(arrow_type):
        return DateType()
    elif pa.types.is_time(arrow_type):
        if arrow_type.unit != "us":
            raise TypeError(
                f"Iceberg time type only supports 'us' precision. Requested precision={arrow_type.unit}'."
            )
        return TimeType()
    elif pa.types.is_timestamp(arrow_type):
        if arrow_type.unit == "ns":
            raise TypeError("Iceberg v1 & v2 does not support timestamps in 'ns' precision.")
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


def arrow_field_to_iceberg(column_id: int, arrow_field: pa.Field) -> NestedField:
    return NestedField(
        field_id=column_id,
        name=arrow_field.name,
        field_type=arrow_type_to_iceberg(arrow_field.type),
        required=not arrow_field.nullable,
    )


def create_schema(arrow_schema: pa.Schema, identifier_fields: Sequence[str] = ()) -> Schema:
    """Convert a pyarrow schema into an iceberg schema

    :param arrow_schema: A pyarrow schema.
    :param identifier_fields: An optional list of fields to mark as identifiers
    """
    iceberg_fields, identifier_field_ids = [], []
    for index, arrow_field in enumerate(arrow_schema):
        col_id = index + 1
        iceberg_fields.append(arrow_field_to_iceberg(col_id, arrow_field))
        if arrow_field.name in identifier_fields:
            identifier_field_ids.append(col_id)

    return Schema(*iceberg_fields, identifier_field_ids=identifier_field_ids)


def evolve_schema(iceberg_schema: Schema, new_arrow_schema: pa.Schema) -> Schema | None:
    """Attempt to evolve the schema to match the data.

    Returns the new schema if updates were applied, else None
    """
    existing_columns = set(iceberg_schema.column_names)
    new_columns = set(new_arrow_schema.names) - existing_columns
    if new_columns:
        num_existing_fields = len(iceberg_schema.fields)

        return Schema(
            *(
                itertools.chain(
                    iceberg_schema.fields,
                    [
                        arrow_field_to_iceberg(
                            num_existing_fields + index + 1, new_arrow_schema.field(name)
                        )
                        for index, name in enumerate(new_arrow_schema.names)
                        if name in new_columns
                    ],
                )
            )
        )
    else:
        return None
