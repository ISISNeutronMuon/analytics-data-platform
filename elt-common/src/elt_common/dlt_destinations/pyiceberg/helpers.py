from typing import Dict, Final, Union

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnType, TTableSchemaColumns
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import (
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.table.sorting import (
    UNSORTED_SORT_ORDER,
    SortOrder,
    SortField,
    SortDirection,
)
from pyiceberg.schema import Schema
import pyiceberg.table.sorting as sorting
import pyiceberg.transforms as transforms
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
from pyiceberg.exceptions import NoSuchNamespaceError


PARTITION_HINT: Final[str] = "x-pyiceberg-partition"
SORT_ORDER_HINT: Final[str] = "x-pyiceberg-sortorder"

TIMESTAMP_PRECISION_TO_UNIT: Dict[int, str] = {0: "s", 3: "ms", 6: "us", 9: "ns"}
UNIT_TO_TIMESTAMP_PRECISION: Dict[str, int] = {v: k for k, v in TIMESTAMP_PRECISION_TO_UNIT.items()}


###############################################################################
# Catalog
###############################################################################


def create_catalog(name: str, **properties: str) -> Catalog:
    """Create an Iceberg catalog

    Args:
        name: Name to identify the catalog.
        properties: Properties that are passed along to the configuration.
    """

    return RestCatalog(name, **properties)


def namespace_exists(catalog: Catalog, namespace: Union[str, Identifier]) -> bool:
    try:
        catalog.load_namespace_properties(namespace)
        return True
    except NoSuchNamespaceError:
        return False


###############################################################################
# Schema
###############################################################################


def dlt_type_to_iceberg(column: TColumnType) -> PrimitiveType:
    """Returns the Iceberg type for the given dlt column data type

    :raises TypeError: If the type is unknown or is not supported
    """
    # dlt types defined in dlt.common.data_types.typing
    dlt_type = column.get("data_type")
    if dlt_type == "bool":
        return BooleanType()
    elif dlt_type == "bigint":
        return LongType()
    elif dlt_type == "double":
        return DoubleType()
    elif dlt_type == "decimal":
        try:
            return DecimalType(column["precision"], column.get("scale"))  # type: ignore
        except KeyError:
            missing = [key for key in ("precision", "scale") if key not in column]
            raise TypeError(
                f"Column with decimal dlt type cannot be created, missing fields: {missing}"
            )
    elif dlt_type == "text" or dlt_type == "json":
        return StringType()
    elif dlt_type == "date":
        return DateType()
    elif dlt_type == "time":
        if "precision" in column and column["precision"] != 6:
            raise TypeError(
                f"Iceberg time type only supports 'us' precision. Requested precision={column['precision']}'."  # type:ignore
            )
        return TimeType()
    elif dlt_type == "timestamp":
        if "precision" in column and column["precision"] == 9:
            raise TypeError(
                f"Iceberg v1 & v2 does not support timestamps in '{TIMESTAMP_PRECISION_TO_UNIT[9]}' precision."  # type:ignore
            )
        if column.get("timezone", True):
            return TimestamptzType()
        else:
            return TimestampType()
    elif dlt_type == "binary":
        return BinaryType()
    else:
        raise TypeError(f"Column in source with dlt type '{dlt_type}' unsupported by Iceberg.")


def iceberg_to_dlt_type(field: NestedField) -> TColumnType:
    """Returns the dlt type string for the given Iceberg field type

    :raises TypeError: If the type is unknown or is not supported
    """
    field_type = field.field_type
    dlt_type: TColumnType = {"nullable": not field.required}
    if isinstance(field_type, BooleanType):
        dlt_type["data_type"] = "bool"
    elif isinstance(field_type, LongType):
        dlt_type["data_type"] = "bigint"
    elif isinstance(field_type, DoubleType):
        dlt_type["data_type"] = "double"
    elif isinstance(field_type, DecimalType):
        dlt_type["data_type"] = "decimal"
        dlt_type["precision"] = field_type.precision
        dlt_type["scale"] = field_type.scale
    elif isinstance(field_type, StringType):
        dlt_type["data_type"] = "text"
    elif isinstance(field_type, DateType):
        dlt_type["data_type"] = "date"
    elif isinstance(field_type, TimeType):
        dlt_type["data_type"] = "time"
    elif isinstance(field_type, TimestampType):
        dlt_type["data_type"] = "timestamp"
        dlt_type["precision"] = 6
        dlt_type["timezone"] = False
    elif isinstance(field_type, TimestamptzType):
        dlt_type["data_type"] = "timestamp"
        dlt_type["precision"] = 6
        dlt_type["timezone"] = True
    elif isinstance(field_type, BinaryType):
        dlt_type["data_type"] = "binary"
    else:
        raise TypeError(
            f"Iceberg type '{field_type}' does not have a corresponding dlt type or is not supported."
        )

    return dlt_type


def create_iceberg_schema(dlt_schema: PreparedTableSchema) -> Schema:
    """Create a Iceberg schema based on a dlt schema

    :param dlt_schema: A dlt schema describing the table.
    """
    columns: TTableSchemaColumns = dlt_schema["columns"]  # type: ignore

    fields, identifier_field_ids = [], []
    for index, (col_name, column) in enumerate(columns.items()):
        col_id = index + 1
        fields.append(
            NestedField(
                col_id,
                col_name,
                dlt_type_to_iceberg(column),
                required=not column.get("nullable", True),
            )
        )
        if column.get("primary_key", False):
            identifier_field_ids.append(col_id)

    return Schema(*fields, identifier_field_ids=identifier_field_ids)


class PartitionTransformation:
    transform: str
    """The transform as a string representation understood by pyicberg.transforms.parse_transform., e.g. `bucket[16]`"""
    column_name: str
    """Column name to apply the transformation to"""

    def __init__(self, transform: str, column_name: str) -> None:
        self.transform = transform
        self.column_name = column_name


class PartitionTrBuilder:
    """Helper class to generate iceberg partition transformations"""

    @staticmethod
    def identity(column_name: str) -> PartitionTransformation:
        """Partition by column without an transformation"""
        return PartitionTransformation(transforms.IDENTITY, column_name)

    @staticmethod
    def year(column_name: str) -> PartitionTransformation:
        """Partition by year part of a date or timestamp column."""
        return PartitionTransformation(transforms.YEAR, column_name)

    @staticmethod
    def month(column_name: str) -> PartitionTransformation:
        """Partition by month part of a date or timestamp column."""
        return PartitionTransformation(transforms.MONTH, column_name)

    @staticmethod
    def day(column_name: str) -> PartitionTransformation:
        """Partition by day part of a date or timestamp column."""
        return PartitionTransformation(transforms.DAY, column_name)

    @staticmethod
    def hour(column_name: str) -> PartitionTransformation:
        """Partition by hour part of a date or timestamp column."""
        return PartitionTransformation(transforms.HOUR, column_name)

    # NOTE: The following transformations are not currently supported by writing through
    # pyarrow so they are disabled.

    # @staticmethod
    # def bucket(n: int, column_name: str) -> PartitionTransformation:
    #     """Partition by hashed value to n buckets."""
    #     return PartitionTransformation(f"{transforms.BUCKET}[{n}]", column_name)

    # @staticmethod
    # def truncate(length: int, column_name: str) -> PartitionTransformation:
    #     """Partition by value truncated to length."""
    #     return PartitionTransformation(f"{transforms.TRUNCATE}[{length}]", column_name)


class SortOrderSpecification:
    direction: str
    """The direction to apply to the sort"""
    column_name: str
    """Column name to apply the transformation to"""

    def __init__(self, direction: str, column_name: str) -> None:
        self.direction = direction
        self.column_name = column_name


class SortOrderBuilder:
    """Builder to generate iceberg sort order specs.

    Note: This only affects the order in which the data is written and not the final
    query. Queries still need to include any ORDER BY clauses if necessary.
    """

    def __init__(self, column_name: str) -> None:
        self.column_name = column_name
        self._direction = None

    @property
    def direction(self) -> str:
        if self._direction is None:
            raise ValueError(
                "Sort direction not specified. Use .asc()/.desc() to indicate the required sort direction."
            )
        return self._direction

    def asc(self) -> "SortOrderBuilder":
        self._direction = sorting.SortDirection.ASC.value
        return self

    def desc(self) -> "SortOrderBuilder":
        self._direction = sorting.SortDirection.DESC.value
        return self

    def build(self) -> SortOrderSpecification:
        return SortOrderSpecification(self.direction, self.column_name)

    # @staticmethod
    # def ascending(transform: PartitionTransformation) -> SortOrderSpecification:
    # @staticmethod
    # def identity(
    #     column_name: str, direction: str, null_order: str
    # ) -> SortOrderSpecification:
    #     """Sort by a column without a transformation"""
    #     return SortOrderSpecification(transforms.IDENTITY, column_name)


def create_partition_spec(dlt_schema: PreparedTableSchema, iceberg_schema: Schema) -> PartitionSpec:
    """Create an Iceberg partition spec for this table if the partition hints
    have been provided"""

    def field_name(column_name: str, transform: str):
        bracket_index = transform.find("[")
        return f"{column_name}_{transform[:bracket_index] if bracket_index > 0 else transform}"

    partition_hint: Dict[str, str] | None = dlt_schema.get(PARTITION_HINT)
    if partition_hint is None:
        return UNPARTITIONED_PARTITION_SPEC

    return PartitionSpec(
        *(
            PartitionField(
                source_id=iceberg_schema.find_field(column_name).field_id,
                field_id=1000 + index,  # the documentation does this...
                transform=transforms.parse_transform(transform),
                name=field_name(column_name, transform),
            )
            for index, (column_name, transform) in enumerate(partition_hint.items())
        )
    )


def create_sort_order(dlt_schema: PreparedTableSchema, iceberg_schema: Schema) -> SortOrder:
    """If the table specifies hints to a Iceberg sort order, create the appropriate
    SortOrder instance.
    """
    sort_order_hint: Dict[str, str] | None = dlt_schema.get(SORT_ORDER_HINT)
    if sort_order_hint is None:
        return UNSORTED_SORT_ORDER

    return SortOrder(
        *(
            SortField(
                source_id=iceberg_schema.find_field(column_name).field_id,
                direction=SortDirection(direction),
                transform=transforms.parse_transform("identity"),
            )
            for column_name, direction in sort_order_hint.items()
        )
    )
