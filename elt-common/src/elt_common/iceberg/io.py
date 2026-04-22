"""Direct Iceberg table writer using pyiceberg.

Provides :class:`IcebergWriter` which handles table creation, schema
evolution, and writing Arrow data to Iceberg tables via ``append``,
``upsert``, or ``replace`` operations.
"""

import logging

import pyarrow as pa
from elt_common.iceberg.catalog import Catalog
from elt_common.iceberg.schema import create_schema, evolve_schema
from elt_common.iceberg.partition import create_partition_spec
from elt_common.iceberg.sortorder import create_sort_order
from elt_common.typing import PartitionHint, SortOrderHint, WriteMode
from pyiceberg.table import ALWAYS_TRUE, Table as IcebergTable
from pyiceberg.typedef import Identifier

logger = logging.getLogger(__name__)


class IcebergIO:
    """Read/write arrow tables to/from Iceberg, handling table creation and schema evolution."""

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    def ensure_namespace(self, namespace: str) -> None:
        """Create the namespace if it doesn't already exist."""
        if not self.catalog.namespace_exists(namespace):
            self.catalog.create_namespace(namespace)
            logger.info(f"Created namespace '{namespace}'")

    def write_table(
        self,
        table_id: Identifier,
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
        if data.num_rows == 0:
            logger.info(f"No data to write to {table_id}, skipping.")
            return

        iceberg_table = self._ensure_table(table_id, data.schema, partition, sort_order)

        with iceberg_table.transaction() as txn:
            if mode == "append":
                iceberg_table.append(data)
            elif mode == "merge":
                if not merge_on:
                    raise ValueError(f"{table_id}: 'merge_on' must be provided when mode='merge'")
                iceberg_table.upsert(
                    df=data,
                    join_cols=merge_on,
                    when_matched_update_all=True,
                    when_not_matched_insert_all=True,
                    case_sensitive=True,
                )
            elif mode == "replace":
                txn.delete(delete_filter=ALWAYS_TRUE)
                logger.info(f"Deleted all records from {table_id}")
                txn.append(data)
            else:
                raise ValueError(f"Unsupported write mode: {mode!r}")

        logger.info(f"Wrote {data.num_rows} rows to {table_id} (mode={mode})")

    # private
    def _ensure_table(
        self,
        table_id: Identifier,
        arrow_schema: pa.Schema,
        partition: PartitionHint | None,
        sort_order: SortOrderHint | None,
    ) -> IcebergTable:
        """Load an existing table or create a new one.

        For existing tables ensure the schema matches the incoming data."""
        if self.catalog.table_exists(table_id):
            return self._ensure_table_schema(self.catalog.load_table(table_id), arrow_schema)

        iceberg_schema = create_schema(arrow_schema)
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

    def _ensure_table_schema(
        self, iceberg_table: IcebergTable, new_schema: pa.Schema
    ) -> IcebergTable:
        """Ensure the existing table schema matches the new schema."""
        new_schema = evolve_schema(iceberg_table.schema(), new_schema)  # type:ignore
        if new_schema is not None:
            logger.debug(f"Evolving schema. New schema: {new_schema}")
            with iceberg_table.update_schema() as update:
                update.union_by_name(new_schema)

        return iceberg_table
