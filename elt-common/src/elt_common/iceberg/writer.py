"""Direct Iceberg table writer using pyiceberg.

Provides :class:`IcebergWriter` which handles table creation, schema
evolution, and writing Arrow data to Iceberg tables via ``append``,
``upsert``, or ``replace`` operations.
"""

import logging

import pyarrow as pa
from elt_common.iceberg.schema import create_iceberg_schema
from elt_common.iceberg.partition import create_partition_spec
from elt_common.iceberg.sortorder import create_sort_order
from elt_common.typing import PartitionHint, SortOrderHint, WriteMode
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import ALWAYS_TRUE, Table as IcebergTable
from pyiceberg.typedef import Identifier

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------


def namespace_exists(catalog: Catalog, namespace: str) -> bool:
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

    def table_id(self, name: str) -> Identifier:
        return (self.namespace, name)

    def ensure_namespace(self) -> None:
        """Create the namespace if it doesn't already exist."""
        if not namespace_exists(self.catalog, self.namespace):
            self.catalog.create_namespace(self.namespace)
            logger.info(f"Created namespace '{self.namespace}'")

    def load_table(self, table_name: str) -> IcebergTable | None:
        """Load an existing Iceberg table, or ``None`` if it doesn't exist."""
        try:
            return self.catalog.load_table(self.table_id(table_name))
        except NoSuchTableError:
            return None

    def write_table(
        self,
        table_name: str,
        data: pa.Table,
        *,
        mode: WriteMode = "append",
        merge_on: list[str] | None = None,
        partition: PartitionHint | None = None,
        sort_order: SortOrderHint | None = None,
    ) -> None:
        """Write an Arrow table to an Iceberg table.

        :param table_name: Name of the table within the namespace.
        :param data: Arrow table containing the data to write.
        :param mode: ``"append"``, ``"merge"``, or ``"replace"``.
        :param merge_on: Column names to join on when ``mode="merge"``.
        :param partition: ``{column_name: transform}`` dict for partitioning.
        :param sort_order: ``{column_name: direction}`` dict for sort order.
        """
        table_id = (self.namespace, table_name)
        if data.num_rows == 0:
            logger.info(f"No data to write to {table_id}, skipping.")
            return

        iceberg_table = self._evolve_schema(
            self._ensure_table(table_id, data.schema, partition, sort_order), data
        )

        if mode == "append":
            iceberg_table.append(data)
        elif mode == "merge":
            if not merge_on:
                raise ValueError("'merge_on' must be provided when mode='merge'")
            iceberg_table.upsert(
                df=data,
                join_cols=merge_on,
                when_matched_update_all=True,
                when_not_matched_insert_all=True,
                case_sensitive=True,
            )
        elif mode == "replace":
            with iceberg_table.transaction() as txn:
                txn.delete(delete_filter=ALWAYS_TRUE)
                logger.info(f"Deleted all records from {self.namespace}.{table_name}")
                txn.append(data)
        else:
            raise ValueError(f"Unsupported write mode: {mode!r}")

        logger.info(f"Wrote {data.num_rows} rows to {self.namespace}.{table_name} (mode={mode})")

    def _ensure_table(
        self,
        table_id: tuple[str, str],
        arrow_schema: pa.Schema,
        partition: PartitionHint | None,
        sort_order: SortOrderHint | None,
    ) -> IcebergTable:
        """Load an existing table or create a new one."""
        if self.catalog.table_exists(table_id):
            return self.catalog.load_table(table_id)

        iceberg_schema = create_iceberg_schema(arrow_schema)
        logger.debug(f"Created iceberg schema: {iceberg_schema}")
        partition_spec = create_partition_spec(partition, iceberg_schema)
        logger.debug(f"Created partition spec: {partition_spec}")
        sort_order_spec = create_sort_order(sort_order, iceberg_schema)
        logger.debug(f"Created sort order spec: {sort_order_spec}")

        logger.info(f"Creating table {table_id}")
        return self.catalog.create_table(
            table_id,
            schema=iceberg_schema,
            partition_spec=partition_spec,
            sort_order=sort_order_spec,
        )

    def _evolve_schema(self, iceberg_table: IcebergTable, new_data: pa.Table) -> IcebergTable:
        """Attempt to evolve the schema to match the data"""
        existing_columns = set(iceberg_table.schema().column_names)
        new_columns = set(new_data.schema.names) - existing_columns
        if new_columns:
            logger.info(f"Evolving schema for {iceberg_table.name}: adding columns {new_columns}")
            new_schema = Schema(
                *[f for f in iceberg_table.schema().fields]
                + [
                    new_data.schema.field(name)
                    for name in new_data.schema.names
                    if name in new_columns
                ]
            )
            with iceberg_table.update_schema() as update:
                update.union_by_name(new_schema)

        return iceberg_table
