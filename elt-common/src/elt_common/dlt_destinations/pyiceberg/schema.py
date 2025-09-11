from typing import Dict, Optional, Iterable, Tuple, cast

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.common.schema.typing import TColumnSchema, TColumnType
from dlt.common.schema.utils import is_nullable_column
from dlt.destinations.type_mapping import TypeMapperImpl

from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.partitioning import (
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.table.name_mapping import MappedField, NameMapping
from pyiceberg.table.sorting import (
    UNSORTED_SORT_ORDER,
    SortOrder,
    SortField,
    SortDirection,
)
from pyiceberg.transforms import parse_transform
from pyiceberg.schema import Schema as PyIcebergSchema

from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    PARTITION_HINT,
    SORT_ORDER_HINT,
)

TIMESTAMP_PRECISION_TO_UNIT: Dict[int, str] = {0: "s", 3: "ms", 6: "us", 9: "ns"}
UNIT_TO_TIMESTAMP_PRECISION: Dict[str, int] = {v: k for k, v in TIMESTAMP_PRECISION_TO_UNIT.items()}


# We use pyarrow types instead of pyiceberg types so that we use the same
# type mappings whether we create an Arrow table or an Iceberg table
class PyIcebergTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {  # type: ignore
        "text": pa.string(),
        "double": pa.float64(),
        "bool": pa.bool_(),
        "bigint": pa.int64(),
        "binary": pa.binary(),
        "date": pa.date32(),
        "json": pa.string(),
    }

    sct_to_dbt = {}

    dbt_to_sct = {  # type: ignore
        pa.string(): "text",
        pa.float64(): "double",
        pa.bool_(): "bool",
        pa.int64(): "bigint",
        pa.binary(): "binary",
        pa.date32(): "date",
    }

    def to_db_decimal_type(self, column: TColumnSchema) -> pa.Decimal128Type:
        precision, scale = self.decimal_precision(column.get("precision"), column.get("scale"))
        return pa.decimal128(precision, scale)

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> pa.TimestampType:
        timezone = column.get("timezone")
        precision = column.get("precision")
        if timezone is not None or (
            precision is not None and precision != self.capabilities.timestamp_precision
        ):
            column_name = column.get("name")
            table_name = table["name"]
            raise TerminalValueError(
                "PyIceberg does not currently support column flags for timezone or a non-default precision."
                f" Flags 'timezone={timezone}' & 'precision={precision}' were used in column '{column_name}' in table '{table_name}'"
            )
        unit: str = TIMESTAMP_PRECISION_TO_UNIT[self.capabilities.timestamp_precision]
        return pa.timestamp(unit, "UTC")

    def to_db_time_type(
        self, column: TColumnSchema, table: PreparedTableSchema = None
    ) -> pa.Time64Type:
        unit: str = TIMESTAMP_PRECISION_TO_UNIT[self.capabilities.timestamp_precision]
        return pa.time64(unit)

    def from_destination_type(
        self,
        db_type: pa.DataType,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> TColumnType:
        if isinstance(db_type, pa.TimestampType):
            return dict(
                data_type="timestamp",
                precision=UNIT_TO_TIMESTAMP_PRECISION[db_type.unit],
                scale=scale,
            )  # type: ignore
        if isinstance(db_type, pa.Time64Type):
            return dict(
                data_type="time",
                precision=UNIT_TO_TIMESTAMP_PRECISION[db_type.unit],
                scale=scale,
            )  # type: ignore
        if isinstance(db_type, pa.Decimal128Type):
            precision, scale = db_type.precision, db_type.scale
            if (precision, scale) == self.capabilities.wei_precision:
                return cast(TColumnType, dict(data_type="wei"))
            return dict(data_type="decimal", precision=precision, scale=scale)  # type: ignore

        return super().from_destination_type(db_type, precision, scale)

    def create_pyiceberg_schema(
        self,
        columns: Iterable[TColumnSchema],
        table: PreparedTableSchema,
    ) -> Tuple[PyIcebergSchema, PartitionSpec, SortOrder]:
        """Create a pyiceberg.Schema from the dlt column & table schemas.

        columns: Iterable[TColumnSchema],
        table: PreparedTableSchema,

        :return: A new Schema
        """
        pyarrow_schema = self.create_pyarrow_schema(columns, table)
        name_mapping = NameMapping(
            [
                MappedField(field_id=index + 1, names=[column["name"]])  # type: ignore
                for index, column in enumerate(columns)
            ]
        )
        iceberg_schema = pyarrow_to_schema(pyarrow_schema, name_mapping=name_mapping)
        return (
            iceberg_schema,
            create_partition_spec(table, iceberg_schema),
            create_sort_order(table, iceberg_schema),
        )

    def create_pyarrow_schema(
        self,
        columns: Iterable[TColumnSchema],
        table: PreparedTableSchema,
    ) -> pa.Schema:
        """Create an Arrow Schema from the dlt column & table schemas.

        :param columns: A list of dlt schemas describing the columns
        :param table: The dlt schema describing the table
        :return: A new Schema
        """
        return pa.schema(
            [
                pa.field(
                    column["name"],
                    self.to_destination_type(column, table),  # type: ignore
                    nullable=is_nullable_column(column),
                )
                for column in columns
            ]
        )


def create_partition_spec(
    dlt_schema: PreparedTableSchema, iceberg_schema: PyIcebergSchema
) -> PartitionSpec:
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
                transform=parse_transform(transform),
                name=field_name(column_name, transform),
            )
            for index, (column_name, transform) in enumerate(partition_hint.items())
        )
    )


def create_sort_order(
    dlt_schema: PreparedTableSchema, iceberg_schema: PyIcebergSchema
) -> SortOrder:
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
                transform=parse_transform("identity"),
            )
            for column_name, direction in sort_order_hint.items()
        )
    )
