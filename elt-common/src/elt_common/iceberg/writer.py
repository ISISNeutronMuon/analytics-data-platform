"""Direct Iceberg table writer using pyiceberg.

Provides :class:`IcebergWriter` which handles table creation, schema
evolution, and writing Arrow data to Iceberg tables via ``append``,
``upsert``, or ``replace`` operations.
"""

import logging
from typing import Literal, Sequence

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.partitioning import (
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import (
    UNSORTED_SORT_ORDER,
    SortDirection,
    SortField,
    SortOrder,
)
import pyiceberg.transforms as transforms

logger = logging.getLogger(__name__)

WriteMode = Literal["append", "merge", "replace"]


# ---------------------------------------------------------------------------
# Partition helpers (ported from dlt_destinations/pyiceberg/helpers.py)
# ---------------------------------------------------------------------------


class PartitionTransformation:
    """A partition transform to apply to a column."""

    def __init__(self, transform: str, column_name: str) -> None:
        self.transform = transform
        self.column_name = column_name


class PartitionTrBuilder:
    """Helper to build common Iceberg partition transformations."""

    @staticmethod
    def identity(column_name: str) -> PartitionTransformation:
        return PartitionTransformation(transforms.IDENTITY, column_name)

    @staticmethod
    def year(column_name: str) -> PartitionTransformation:
        return PartitionTransformation(transforms.YEAR, column_name)

    @staticmethod
    def month(column_name: str) -> PartitionTransformation:
        return PartitionTransformation(transforms.MONTH, column_name)

    @staticmethod
    def day(column_name: str) -> PartitionTransformation:
        return PartitionTransformation(transforms.DAY, column_name)

    @staticmethod
    def hour(column_name: str) -> PartitionTransformation:
        return PartitionTransformation(transforms.HOUR, column_name)


class SortOrderSpecification:
    """A sort specification to apply to a column."""

    def __init__(self, direction: str, column_name: str) -> None:
        self.direction = direction
        self.column_name = column_name


class SortOrderBuilder:
    """Builder to generate Iceberg sort order specs."""

    def __init__(self, column_name: str) -> None:
        self.column_name = column_name
        self._direction: str | None = None

    @property
    def direction(self) -> str:
        if self._direction is None:
            raise ValueError("Sort direction not specified. Use .asc()/.desc() to set direction.")
        return self._direction

    def asc(self) -> "SortOrderBuilder":
        self._direction = SortDirection.ASC.value
        return self

    def desc(self) -> "SortOrderBuilder":
        self._direction = SortDirection.DESC.value
        return self

    def build(self) -> SortOrderSpecification:
        return SortOrderSpecification(self.direction, self.column_name)


def build_partition_spec(
    partition: dict[str, str] | None,
    iceberg_schema: Schema,
) -> PartitionSpec:
    """Build an Iceberg ``PartitionSpec`` from a ``{column: transform}`` dict."""
    if not partition:
        return UNPARTITIONED_PARTITION_SPEC

    def _field_name(column_name: str, transform: str) -> str:
        bracket = transform.find("[")
        return f"{column_name}_{transform[:bracket] if bracket > 0 else transform}"

    return PartitionSpec(
        *(
            PartitionField(
                source_id=iceberg_schema.find_field(col).field_id,
                field_id=1000 + idx,
                transform=transforms.parse_transform(tr),
                name=_field_name(col, tr),
            )
            for idx, (col, tr) in enumerate(partition.items())
        )
    )


def build_sort_order(
    sort_order: dict[str, str] | None,
    iceberg_schema: Schema,
) -> SortOrder:
    """Build an Iceberg ``SortOrder`` from a ``{column: direction}`` dict."""
    if not sort_order:
        return UNSORTED_SORT_ORDER

    return SortOrder(
        *(
            SortField(
                source_id=iceberg_schema.find_field(col).field_id,
                direction=SortDirection(direction),
                transform=transforms.parse_transform("identity"),
            )
            for col, direction in sort_order.items()
        )
    )


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------


def namespace_exists(catalog: Catalog, namespace: str | tuple[str, ...]) -> bool:
    """Check if a namespace exists in the catalog."""
    try:
        catalog.load_namespace_properties(namespace)
        return True
    except NoSuchNamespaceError:
        return False


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class IcebergWriter:
    """Writes Arrow tables to Iceberg, handling table creation and schema evolution."""

    def __init__(self, catalog: Catalog, namespace: str) -> None:
        self.catalog = catalog
        self.namespace = namespace

    def ensure_namespace(self) -> None:
        """Create the namespace if it doesn't already exist."""
        if not namespace_exists(self.catalog, self.namespace):
            self.catalog.create_namespace(self.namespace)
            logger.info(f"Created namespace '{self.namespace}'")

    def load_table(self, table_name: str) -> Table | None:
        """Load an existing Iceberg table, or ``None`` if it doesn't exist."""
        try:
            return self.catalog.load_table((self.namespace, table_name))
        except NoSuchTableError:
            return None

    def write_table(
        self,
        table_name: str,
        data: pa.Table,
        *,
        mode: WriteMode = "append",
        merge_on: Sequence[str] | None = None,
        partition: dict[str, str] | None = None,
        sort_order: dict[str, str] | None = None,
    ) -> None:
        """Write an Arrow table to an Iceberg table.

        :param table_name: Name of the table within the namespace.
        :param data: Arrow table containing the data to write.
        :param mode: ``"append"``, ``"merge"``, or ``"replace"``.
        :param merge_on: Column names to join on when ``mode="merge"``.
        :param partition: ``{column_name: transform}`` dict for partitioning.
        :param sort_order: ``{column_name: direction}`` dict for sort order.
        """
        if data.num_rows == 0:
            logger.info(f"No data to write to {self.namespace}.{table_name}, skipping.")
            return

        table_id = (self.namespace, table_name)
        iceberg_table = self._ensure_table(table_id, data.schema, partition, sort_order)

        # Schema evolution: add any new columns
        existing_columns = set(iceberg_table.schema().column_names)
        new_columns = set(data.schema.names) - existing_columns
        if new_columns:
            logger.info(f"Evolving schema for {table_name}: adding columns {new_columns}")
            new_schema = Schema(
                *[f for f in iceberg_table.schema().fields]
                + [data.schema.field(name) for name in data.schema.names if name in new_columns]
            )
            with iceberg_table.update_schema() as update:
                update.union_by_name(new_schema)

        if mode == "append":
            iceberg_table.append(data)
        elif mode == "merge":
            if not merge_on:
                raise ValueError("merge_on must be provided when mode='merge'")
            iceberg_table.upsert(
                df=data,
                join_cols=merge_on,
                when_matched_update_all=True,
                when_not_matched_insert_all=True,
                case_sensitive=True,
            )
        elif mode == "replace":
            iceberg_table.delete()
            iceberg_table.append(data)
        else:
            raise ValueError(f"Unsupported write mode: {mode!r}")

        logger.info(f"Wrote {data.num_rows} rows to {self.namespace}.{table_name} (mode={mode})")

    def _ensure_table(
        self,
        table_id: tuple[str, str],
        arrow_schema: pa.Schema,
        partition: dict[str, str] | None,
        sort_order: dict[str, str] | None,
    ) -> Table:
        """Load an existing table or create a new one."""
        if self.catalog.table_exists(table_id):
            return self.catalog.load_table(table_id)

        # Infer Iceberg schema from Arrow schema
        iceberg_schema = Schema()
        partition_spec = build_partition_spec(partition, iceberg_schema)
        sort_order_spec = build_sort_order(sort_order, iceberg_schema)

        logger.info(f"Creating table {table_id}")
        return self.catalog.create_table(
            table_id,
            schema=arrow_schema,
            partition_spec=partition_spec,
            sort_order=sort_order_spec,
        )
