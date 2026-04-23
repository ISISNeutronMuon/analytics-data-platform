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
from elt_common.typing import PartitionConfig, SortOrderConfig, TableProperties
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
        props: TableProperties,
        data: pa.Table,
    ) -> None:
        """Write an Arrow table to an Iceberg table.

        :param table_id: Full Identifier for table in catalog.
        :param props: Describe properties for writing to the table
        :param data: Arrow table containing the data to write.
        """
        if data.num_rows == 0:
            logger.info(f"No data to write to {table_id}, skipping.")
            return

        iceberg_table = self._ensure_table(table_id, data.schema, props.partition, props.sort_order)

        with iceberg_table.transaction() as txn:
            if props.write_mode == "append":
                iceberg_table.append(data)
            elif props.write_mode == "merge":
                iceberg_table.upsert(
                    df=data,
                    join_cols=props.merge_on,
                    when_matched_update_all=True,
                    when_not_matched_insert_all=True,
                    case_sensitive=True,
                )
            elif props.write_mode == "replace":
                txn.delete(delete_filter=ALWAYS_TRUE)
                logger.info(f"Deleted all records from {table_id}")
                txn.append(data)
            else:
                raise ValueError(f"Unsupported write mode: {props.write_mode!r}")

        logger.debug(f"Wrote {data.num_rows} rows to {table_id} (mode={props.write_mode})")

    # private
    def _ensure_table(
        self,
        table_id: Identifier,
        arrow_schema: pa.Schema,
        partition: PartitionConfig | None,
        sort_order: SortOrderConfig | None,
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
