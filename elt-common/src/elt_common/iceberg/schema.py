from typing import Sequence

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
)


def arrow_type_to_iceberg(arrow_type: pa.DataType, field_id: int = 1) -> IcebergType:
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

    elif pa.types.is_list(arrow_type):
        # The list itself uses field_id, the lists element type uses the subsequent id
        element_type = arrow_type_to_iceberg(arrow_type.value_type, field_id + 1)

        # HACK: element_required is set to false because of difficulties getting object
        # list elements from JSON to be optional.
        # Not sure if this is a limitation of pyarrow or I just didn't find the right incantation
        return ListType(element_id=field_id, element_type=element_type, element_required=False)

    elif pa.types.is_struct(arrow_type):
        iceberg_fields = []
        next_field_id = field_id
        for subfield in arrow_type.fields:
            iceberg_field = arrow_field_to_iceberg(arrow_field=subfield, column_id=next_field_id)
            next_field_id = get_max_field_id(iceberg_field) + 1
            iceberg_fields.append(iceberg_field)

        return StructType(*iceberg_fields)
    else:
        raise TypeError(f"Pyarrow type '{arrow_type}' unknown to type mapper.")


def arrow_field_to_iceberg(column_id: int, arrow_field: pa.Field) -> NestedField:
    return NestedField(
        field_id=column_id,
        name=arrow_field.name,
        field_type=arrow_type_to_iceberg(arrow_field.type, column_id + 1),
        required=not arrow_field.nullable,
    )


def create_schema(arrow_schema: pa.Schema, identifier_fields: Sequence[str] = ()) -> Schema:
    """Convert a pyarrow schema into an iceberg schema

    :param arrow_schema: A pyarrow schema.
    :param identifier_fields: An optional list of fields to mark as identifiers
    """
    iceberg_fields, identifier_field_ids = [], []
    col_id = 1
    for arrow_field in arrow_schema:
        field = arrow_field_to_iceberg(col_id, arrow_field)

        iceberg_fields.append(field)
        if arrow_field.name in identifier_fields:
            identifier_field_ids.append(col_id)

        col_id = get_max_field_id(field) + 1

    return Schema(*iceberg_fields, identifier_field_ids=identifier_field_ids)


def evolve_schema(iceberg_schema: Schema, new_arrow_schema: pa.Schema) -> Schema | None:
    """Attempt to evolve the schema to match the data.

    Only new fields are considered backwards compatible. This is less permissive
    than should be allowed - renaming fields, reordering files, and some type/property
    changes could also be allowed - but iceberg rejects the changes when trying
    to actually write to the table.

    :returns: None if the schema didn't change, or the new schema if it did (in a backward compatible way).
    :raises ValueError: If the schema has incompatible changes.
    """
    new_iceberg_schema = create_schema(new_arrow_schema)

    if new_iceberg_schema == iceberg_schema:
        return None
    else:
        # If there are incompatible changes, throw an error
        incompatibilities = []
        for f in iceberg_schema.fields:
            try:
                new_field = new_iceberg_schema.find_field(f.field_id)
            except ValueError:
                incompatibilities.append(f"Field id {f.field_id} removed")
                continue

            if f.name != new_field.name:
                incompatibilities.append(
                    f"Field {f.field_id} changed name from '{f.name}' to '{new_field.name}'"
                )
            elif f.field_type != new_field.field_type:
                incompatibilities.append(
                    f"Field '{f.name}' (id: {f.field_id}) changed type from '{f.field_type}' to '{new_field.field_type}'"
                )
            elif new_field.required != f.required:
                incompatibilities.append(
                    f"Field '{f.name}' (id: {f.field_id}) 'required' changed to {new_field.required}"
                )

        if incompatibilities:
            raise ValueError(f"Incompatible changes to schema: {incompatibilities}")

    # The new schema is different, but backwards compatible
    return new_iceberg_schema


def get_max_field_id(f: NestedField) -> int:
    """Return the largest field_id from an Iceberg field.

    - For primitive fields this is just the field_id
    - For list fields this is the larget field_id from the list's element type
    - For struct fields this is the largest id across all of its subfields (potentially recursively)
    """
    if f.field_type.is_primitive:
        return f.field_id
    elif isinstance(f.field_type, StructType):
        struct_fields = f.field_type.fields
        return max(get_max_field_id(sub) for sub in struct_fields)
    elif isinstance(f.field_type, ListType):
        return get_max_field_id(f.field_type.element_field)
    else:
        raise ValueError("Can only get fields ids for primitive, list, and struct fields")
