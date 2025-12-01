import json
from types import TracebackType
from typing import cast, Iterable, Optional, Sequence, Type, Tuple

from dlt.common import pendulum, logger
from dlt.common.destination.client import (
    JobClientBase,
    LoadJob,
    RunnableLoadJob,
    StateInfo,
    StorageSchemaInfo,
    WithStateSync,
)

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTerminalException,
)
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.destination.utils import resolve_merge_strategy, resolve_replace_strategy
from dlt.common.schema import Schema, TColumnSchema, TSchemaTables, TTableSchemaColumns
from dlt.common.schema.typing import TPartialTableSchema, TWriteDisposition
from dlt.common.utils import uniq_id

from dlt.common.libs.pyarrow import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.expressions import AlwaysTrue, And, EqualTo
from pyiceberg.table import Table as PyIcebergTable

from elt_common.dlt_destinations.pyiceberg.configuration import (
    IcebergClientConfiguration,
)
from elt_common.dlt_destinations.pyiceberg.helpers import (
    create_catalog,
    create_iceberg_schema,
    create_partition_spec,
    create_sort_order,
    namespace_exists,
    Identifier,
    Catalog as PyIcebergCatalog,
)
from elt_common.dlt_destinations.pyiceberg.exceptions import pyiceberg_error
from elt_common.dlt_destinations.pyiceberg.type_mapping import (
    PyIcebergTypeMapper,
)


class PyIcebergLoadJob(RunnableLoadJob):
    def __init__(
        self,
        file_path: str,
    ) -> None:
        super().__init__(file_path)

    @property
    def iceberg_client(self) -> "PyIcebergClient":
        return cast(PyIcebergClient, self._job_client)

    @property
    def iceberg_catalog(self) -> PyIcebergCatalog:
        return self.iceberg_client.iceberg_catalog

    def run(self):
        self.iceberg_client.write_to_table(
            self.load_table_name,
            pq.read_table(self._file_path),
            self._load_table.get("write_disposition"),
        )


class PyIcebergClient(JobClientBase, WithStateSync):
    def __init__(
        self,
        schema: Schema,
        config: IcebergClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ):
        super().__init__(schema, config, capabilities)
        self.dataset_name = self.config.normalize_dataset_name(self.schema)
        self.config = config
        self.iceberg_catalog = create_catalog(name="default", **config.connection_properties)
        self.type_mapper = cast(PyIcebergTypeMapper, self.capabilities.get_type_mapper())

    # ----- JobClientBase -----
    @pyiceberg_error
    def initialize_storage(self, truncate_tables: Optional[Iterable[str]] = None) -> None:
        """Prepares storage to be used ie. creates database schema or file system folder. Truncates requested tables."""
        if not self.is_storage_initialized():
            self.iceberg_catalog.create_namespace(self.dataset_name)
        elif truncate_tables:
            for table_name in truncate_tables:
                try:
                    self.load_table_from_name(table_name).delete()
                except DestinationUndefinedEntity:
                    pass

    def is_storage_initialized(self) -> bool:
        """Returns if storage is ready to be read/written."""
        return namespace_exists(self.iceberg_catalog, self.dataset_name)

    def drop_storage(self) -> None:
        """Brings storage back into not initialized state. Typically data in storage is destroyed.

        Note: This will remove all tables within the storage area not just those created by any partiular load
        """
        # First purge all tables
        for table_id in self.iceberg_catalog.list_tables(self.dataset_name):
            self.iceberg_catalog.purge_table(table_id)

        # Now drop namespace
        self.iceberg_catalog.drop_namespace(self.dataset_name)

    def is_dlt_table(self, table_name: str) -> bool:
        return table_name.startswith(self.schema._dlt_tables_prefix)

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> TSchemaTables | None:
        applied_update = {}
        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is None:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} not found in the storage."
                " upgrading"
            )
            for table_name in only_tables or self.schema.tables.keys():
                if table_schema := self.create_or_update_table_schema(table_name):
                    applied_update[table_name] = table_schema

            self.update_schema_in_version_table()
        else:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} inserted at"
                f" {schema_info.inserted_at} found in storage, no upgrade required"
            )

        return applied_update

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        table = super().prepare_load_table(table_name)
        merge_strategy = resolve_merge_strategy(self.schema.tables, table, self.capabilities)
        if table["write_disposition"] == "merge":
            if merge_strategy is None:
                # no supported merge strategies, fall back to append
                table["write_disposition"] = "append"
            else:
                table["x-merge-strategy"] = merge_strategy  # type: ignore[typeddict-unknown-key]
        if table["write_disposition"] == "replace":
            replace_strategy = resolve_replace_strategy(
                table, self.config.replace_strategy, self.capabilities
            )
            assert replace_strategy, f"Must be able to get replace strategy for {table_name}"
            table["x-replace-strategy"] = replace_strategy  # type: ignore[typeddict-unknown-key]
        if self.is_dlt_table(table["name"]):
            table.pop("table_format", None)
        return table

    def update_schema_in_version_table(self) -> None:
        """Update the dlt version table with details of the current schema"""
        version_table_dlt_schema = self.schema.tables.get(self.schema.version_table_name)
        version_table_dlt_columns = version_table_dlt_schema["columns"]
        schema = self.schema
        values = [
            schema.version,
            schema.ENGINE_VERSION,
            pendulum.now(),
            schema.name,
            schema.stored_version_hash,
            json.dumps(schema.to_dict()),
        ]
        version_table_identifiers = list(version_table_dlt_columns.keys())
        assert len(values) == len(version_table_identifiers)

        data_to_load = pa.Table.from_pylist(
            [{k: v for k, v in zip(version_table_identifiers, values)}],
            schema=create_iceberg_schema(
                self.prepare_load_table(self.schema.version_table_name),
            ).as_arrow(),
        )
        write_disposition = self.schema.get_table(self.schema.version_table_name).get(
            "write_disposition"
        )
        self.write_to_table(self.schema.version_table_name, data_to_load, write_disposition)

    def create_load_job(
        self,
        table: PreparedTableSchema,
        file_path: str,
        load_id: str,
        restore: bool = False,
    ) -> LoadJob:
        """Creates a load job for a particular `table` with content in `file_path`. Table is already prepared to be loaded."""
        return PyIcebergLoadJob(file_path)

    def complete_load(self, load_id: str) -> None:
        """Marks the load package with `load_id` as completed in the destination. Before such commit is done, the data with `load_id` is invalid."""
        loads_table_dlt_schema = self.schema.tables.get(self.schema.loads_table_name)
        loads_table_dlt_columns = loads_table_dlt_schema["columns"]
        values = [
            load_id,
            self.schema.name,
            0,
            pendulum.now(),
            self.schema.version_hash,
        ]
        loads_table_identifiers = list(loads_table_dlt_columns.keys())
        assert len(values) == len(loads_table_identifiers)

        data_to_load = pa.Table.from_pylist(
            [{k: v for k, v in zip(loads_table_identifiers, values)}],
            schema=create_iceberg_schema(
                self.prepare_load_table(self.schema.loads_table_name),
            ).as_arrow(),
        )

        self.write_to_table(self.schema.loads_table_name, data_to_load)

    # ----- WithStateSync -----
    def get_stored_schema(self, schema_name: str = None) -> StorageSchemaInfo | None:
        """Get the latest schema, optionally specifying a schema_name filter"""
        try:
            version_table = self.load_table_from_name(self.schema.version_table_name)
        except DestinationUndefinedEntity:
            return None

        c_schema_name = self.schema.naming.normalize_identifier("schema_name")
        c_inserted_at = self.schema.naming.normalize_identifier("inserted_at")
        row_filter = AlwaysTrue() if schema_name is None else EqualTo(c_schema_name, schema_name)
        schema_versions = (
            version_table.scan(row_filter=row_filter)
            .to_arrow()
            .sort_by([(c_inserted_at, "descending")])
        )
        try:
            return StorageSchemaInfo.from_normalized_mapping(
                schema_versions.to_pylist()[0], self.schema.naming
            )
        except IndexError:
            return None

    def get_stored_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo | None:  # type: ignore
        """Get the latest schema by schema hash value."""
        try:
            version_table = self.load_table_from_name(self.schema.version_table_name)
        except DestinationUndefinedEntity:
            return None

        c_version_hash = self.schema.naming.normalize_identifier("version_hash")
        c_inserted_at = self.schema.naming.normalize_identifier("inserted_at")

        schema_versions = (
            version_table.scan(row_filter=EqualTo(c_version_hash, version_hash))
            .to_arrow()
            .sort_by([(c_inserted_at, "descending")])
        )
        try:
            return StorageSchemaInfo.from_normalized_mapping(
                schema_versions.to_pylist()[0], self.schema.naming
            )
        except IndexError:
            return None

    def get_stored_state(self, pipeline_name: str) -> StateInfo | None:
        """Loads compressed state from destination storage."""
        try:
            state_table_obj = self.load_table_from_name(self.schema.state_table_name)
            loads_table_obj = self.load_table_from_name(self.schema.loads_table_name)
        except DestinationUndefinedEntity:
            return None

        c_load_id, c_dlt_load_id, c_pipeline_name, c_status = map(
            self.schema.naming.normalize_identifier,
            ("load_id", "_dlt_load_id", "pipeline_name", "status"),
        )

        load_ids = loads_table_obj.scan(
            row_filter=EqualTo(c_status, 0), selected_fields=(c_load_id,)
        ).to_arrow()
        try:
            latest_load_id = load_ids.sort_by([(c_load_id, "descending")]).to_pylist()[0][c_load_id]
            states = state_table_obj.scan(
                row_filter=And(
                    EqualTo(c_dlt_load_id, latest_load_id),
                    EqualTo(c_pipeline_name, pipeline_name),
                ),
            ).to_arrow()

            return StateInfo.from_normalized_mapping(states.to_pylist()[0], self.schema.naming)
        except IndexError:
            # Table exists but there is no data
            return None

    # ----- Helpers  -----
    @pyiceberg_error
    def create_or_update_table_schema(self, table_name: str) -> Optional[TPartialTableSchema]:
        prepared_table = self.prepare_load_table(table_name)
        # table with empty columns will not be created
        if not prepared_table["columns"]:
            return None

        # Most catalogs purge files as background tasks so if tables of the
        # same identifier are created/deleted in tight loops, e.g. in tests,
        # then the same location can produce invalid location errors.
        # The location_tag can be used to set to unique string to avoid this
        if self.config.table_location_layout is not None:
            location = f"{self.config.bucket_url}/" + self.config.table_location_layout.format(  # type: ignore
                dataset_name=self.dataset_name,
                table_name=table_name.rstrip("/"),
                location_tag=uniq_id(6),
            )
        else:
            location = None

        catalog = self.iceberg_catalog
        table_id = self.make_qualified_table_name(table_name)
        required_iceberg_schema = create_iceberg_schema(prepared_table)
        if catalog.table_exists(table_id):
            iceberg_table = catalog.load_table(table_id)
            new_columns = set(prepared_table["columns"].keys()).difference(
                iceberg_table.schema().column_names
            )
            if new_columns:
                with iceberg_table.update_schema() as update:
                    update.union_by_name(required_iceberg_schema)
                prepared_table["columns"] = {
                    k: v for k, v in prepared_table["columns"].items() if k in new_columns
                }
            else:
                prepared_table = None

        else:
            partition_spec, sort_order = (
                create_partition_spec(prepared_table, required_iceberg_schema),
                create_sort_order(prepared_table, required_iceberg_schema),
            )
            self.iceberg_catalog.create_table(
                table_id,
                required_iceberg_schema,
                partition_spec=partition_spec,
                sort_order=sort_order,
                location=location,
            )

        return prepared_table

    def get_storage_tables(
        self, table_names: Iterable[str]
    ) -> Iterable[Tuple[str, TTableSchemaColumns]]:
        """Accesses the catalog and retrieves column information for the given table names, if they exist.
        An empty TTableSchemaColumns indicates in the return Tuple indicates the table does not exist in the
        destination.
        Table names should be normalized according to naming convention.

        This method is modelled on the method of the same name in dlt/destinations/job_client_impl.py
        """
        table_names = list(table_names)
        if len(table_names) == 0:
            # empty generator
            return

        catalog = self.iceberg_catalog
        for table_name in table_names:
            table_id = self.make_qualified_table_name(table_name)
            if catalog.table_exists(table_id):
                storage_columns = {}
                table: PyIcebergTable = catalog.load_table(table_id)
                schema = table.schema()
                for field in schema.fields:
                    column = self.type_mapper.from_destination_type(field)
                    column["name"] = field.name
                    if field.field_id in schema.identifier_field_ids:
                        column["primary_key"] = True
                    storage_columns[field.name] = column

                yield table_name, storage_columns
            else:
                yield table_name, {}

    def compare_table_schemas(
        self, table_name: str, storage_columns: TTableSchemaColumns
    ) -> Sequence[TColumnSchema]:
        """Compares storage columns with schema table and produce delta columns difference"""
        updates = self.schema.get_new_table_columns(
            table_name,
            storage_columns,
            case_sensitive=self.capabilities.generates_case_sensitive_identifiers(),
        )
        logger.info(f"Found {len(updates)} updates for {table_name} in {self.schema.name}")
        return updates

    @pyiceberg_error
    def write_to_table(
        self,
        table_name: str,
        table_data: pa.Table,
        write_disposition: TWriteDisposition | None = "append",
    ):
        """Append a pyarrow Table to an Iceberg table.

        :param qualified_table_name: The fully qualified name of the table
        :param table_data: pyarrow.Table containing the data
        :param write_disposition: One of the standard dlt write_disposition modes
        """
        table_identifier = self.make_qualified_table_name(table_name)
        table = self.iceberg_catalog.load_table(table_identifier)
        if write_disposition in ("append", "replace", "skip"):
            # replace will have triggered the tables to be truncated so we can just append
            # skip is internally used for dlt tables
            table.append(table_data)
        elif write_disposition == "merge":
            strategy = self.prepare_load_table(table_name)["x-merge-strategy"]  # type:ignore
            if strategy == "upsert":
                # requires the indentifier fields to have been defined
                table.upsert(
                    df=table_data,
                    when_matched_update_all=True,
                    when_not_matched_insert_all=True,
                    case_sensitive=True,
                )
            else:
                raise DestinationTerminalException(
                    f'Merge strategy "{strategy}" is not supported for Iceberg tables. '
                    f'Table: "{table_name}".'
                )
        else:
            raise DestinationTerminalException(
                f"Unsupported write disposition {write_disposition} for pyiceberg destination."
            )

    @pyiceberg_error
    def load_table_from_schema(self, schema_table: PreparedTableSchema) -> PyIcebergTable:
        return self.iceberg_catalog.load_table(self.make_qualified_table_name(schema_table["name"]))

    @pyiceberg_error
    def load_table_from_name(self, table_name: str) -> PyIcebergTable:
        return self.iceberg_catalog.load_table(self.make_qualified_table_name(table_name))

    def make_qualified_table_name(self, table_name: str) -> Identifier:
        return (self.dataset_name, table_name)

    # ----- contextmanager -----
    def __enter__(self) -> "JobClientBase":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass
