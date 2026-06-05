"""Iceberg table io using pyiceberg.

Provides :class:`IcebergIO` which can be used to write pyarrow tables to an
iceberg catalog, and read properties from iceberg tables, whilst handling table,
namespace, and schema creation/evolution
"""

import logging

import pyarrow as pa
from pyiceberg.typedef import Identifier

from elt_common.iceberg.catalog import Catalog
from elt_common.iceberg.schema import create_schema, evolve_schema
from elt_common.iceberg.partition import create_partition_spec
from elt_common.iceberg.sortorder import create_sort_order
from elt_common.typing import (
    BaseIO,
    PartitionConfig,
    SortOrderConfig,
    WriteMode,
)
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import ALWAYS_TRUE, Table as IcebergTable

# Map exception 1:1
NoSuchTableError = NoSuchTableError

LOGGER = logging.getLogger(__name__)


class IcebergIO(BaseIO):
    """Read/write arrow tables to/from Iceberg, handling table creation and schema evolution."""

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    def ensure_namespace(self, namespace: str) -> None:
        """Create the namespace if it doesn't already exist."""
        if not self.catalog.namespace_exists(namespace):
            self.catalog.create_namespace(namespace)
            LOGGER.info(f"Created namespace '{namespace}'")

    def read_property(self, table_id: Identifier, key: str) -> str:
        """Read a table property.

        :param table_id: namespaced identifier of the table to read from
        :param key: the key to read the value of
        :raises: KeyError if property does not exist
        """
        table = self.catalog.load_table(table_id)
        return table.properties[key]

    def write_table(
        self,
        table_id: Identifier,
        data: pa.Table,
        write_mode: WriteMode,
        *,
        merge_on: list[str] | None = None,
        partition: PartitionConfig | None = None,
        sort_order: SortOrderConfig | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        """Write an Arrow table to an Iceberg table.

        :param table_id: namespaced identifier of the table to write to
        :param data: the new data to write to the table
        :param write_mode: 'append' adds the data to the table,
                           'merge' adds new data and modifies existing rows,
                           'replace' completely overwrites the table with the new data
        :param merge_on: field(s) to determine if rows should be merged. Required if write_mode is 'merge'
        :param partition: mapping of table names to the column(s) they should be partitioned by
        :param sort_order: mapping of table names to the sort direction of their column(s)
        :param properties: additional properties to set on the table upon completion. Useful for watermarking
        """
        if data.num_rows == 0:
            LOGGER.info(f"No data to write to {table_id}, skipping.")
            return

        iceberg_table = self._ensure_table(table_id, data.schema, partition, sort_order)

        with iceberg_table.transaction() as txn:
            if write_mode == "append":
                txn.append(data)
            elif write_mode == "merge":
                if merge_on is None:
                    raise ValueError(
                        f"Table '{table_id}': write mode 'merge' requires 'merge_on' property."
                    )
                txn.upsert(
                    df=data,
                    join_cols=merge_on,
                    when_matched_update_all=True,
                    when_not_matched_insert_all=True,
                    case_sensitive=True,
                )
            elif write_mode == "replace":
                txn.overwrite(data, overwrite_filter=ALWAYS_TRUE, case_sensitive=True)
            else:
                raise ValueError(f"Unsupported write mode: {write_mode!r}")

            if properties is not None:
                txn.set_properties(properties)

        LOGGER.debug(f"Wrote {data.num_rows} rows to {table_id} (mode={write_mode})")

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
            return _ensure_table_schema(self.catalog.load_table(table_id), arrow_schema)

        iceberg_schema = create_schema(arrow_schema)
        LOGGER.debug(f"Created iceberg schema: {iceberg_schema}")
        partition_spec = create_partition_spec(partition, iceberg_schema)
        LOGGER.debug(f"Created partition spec: {partition_spec}")
        sort_order_spec = create_sort_order(sort_order, iceberg_schema)
        LOGGER.debug(f"Created sort order spec: {sort_order_spec}")

        LOGGER.info(f"Creating table {table_id}")
        return self.catalog.create_table(
            table_id,
            schema=iceberg_schema,
            partition_spec=partition_spec,
            sort_order=sort_order_spec,
        )


def _ensure_table_schema(iceberg_table: IcebergTable, new_schema: pa.Schema) -> IcebergTable:
    """Ensure the existing table schema matches the new schema."""
    new_schema = evolve_schema(iceberg_table.schema(), new_schema)  # type:ignore
    if new_schema is not None:
        LOGGER.debug(f"Evolving schema. New schema: {new_schema}")
        with iceberg_table.update_schema() as update:
            update.union_by_name(new_schema)

    return iceberg_table
