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
from dlt.common.schema import Schema, TColumnSchema, TSchemaTables, TTableSchemaColumns
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.storages.exceptions import SchemaStorageException
from dlt.common.utils import uniq_id

from dlt.common.libs.pyarrow import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.expressions import AlwaysTrue, And, EqualTo
from pyiceberg.table import Table as PyIcebergTable

from pipelines_common.dlt_destinations.pyiceberg.configuration import (
    IcebergClientConfiguration,
)
from pipelines_common.dlt_destinations.pyiceberg.catalog import (
    create_catalog,
    namespace_exists,
    Identifier,
    PyIcebergCatalog,
)
from pipelines_common.dlt_destinations.pyiceberg.exceptions import pyiceberg_error
from pipelines_common.dlt_destinations.pyiceberg.schema import (
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
        self.iceberg_client.write_to_table(self.load_table_name, pq.read_table(self._file_path))


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

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> TSchemaTables | None:
        applied_update = super().update_stored_schema(only_tables, expected_update)
        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is None:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} not found in the storage."
                " upgrading"
            )
            self.execute_destination_schema_update(only_tables)
        else:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} inserted at"
                f" {schema_info.inserted_at} found in storage, no upgrade required"
            )

        return applied_update

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
            schema=self.type_mapper.create_pyarrow_schema(
                version_table_dlt_columns.values(),
                self.prepare_load_table(self.schema.version_table_name),
            ),
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
            schema=self.type_mapper.create_pyarrow_schema(
                loads_table_dlt_columns.values(),
                self.prepare_load_table(self.schema.loads_table_name),
            ),
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
    def execute_destination_schema_update(self, only_tables: Iterable[str]) -> TSchemaTables:
        """Ensure the schemas of the destination tables match what they are expected to be.

        The tables are created if they do not exist or their schemas are evolved if they
        already exist but the schema is out of date.

        :param only_tables: Only `only_tables` are included, or all if None.
        :return: A mapping of table name to newly updated schemas.
        """
        applied_update: TSchemaTables = {}
        for table_name, storage_columns in self.get_storage_tables(
            only_tables or self.schema.tables.keys()
        ):
            new_columns = self.compare_table_schemas(table_name, storage_columns)
            if len(new_columns) > 0:
                if len(storage_columns) > 0:
                    # TODO: Implement schema evolution
                    raise SchemaStorageException(
                        "pyiceberg destination does not currently support schema evolution."
                    )
                table = self.prepare_load_table(table_name)
                self.execute_destination_schema_create_table(new_columns, table)

        self.update_schema_in_version_table()
        return applied_update

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
                schema = table.schema().as_arrow()
                for name in schema.names:
                    field = schema.field(name)
                    storage_columns[name] = {
                        "name": name,
                        **self.type_mapper.from_destination_type(field.type, None, None),
                        "nullable": field.nullable,
                    }
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
    def execute_destination_schema_create_table(
        self, columns: Sequence[TColumnSchema], table: PreparedTableSchema
    ):
        """Create a new table schema in the catalog given the column schemas"""
        schema, partition_spec, sort_order = self.type_mapper.create_pyiceberg_schema(
            columns, table
        )
        table_name: str = table["name"]
        # Most catalogs purge files as background tasks so if tables of the
        # same identifier are created/deleted in tight loops, e.g. in tests,
        # then the same location can produce invalid location errors.
        # The location_tag can be used to set to unique string to avoid this
        location = f"{self.config.bucket_url}/" + self.config.table_location_layout.format(  # type: ignore
            dataset_name=self.dataset_name,
            table_name=table_name.rstrip("/"),
            location_tag=uniq_id(6),
        )
        self.iceberg_catalog.create_table(
            self.make_qualified_table_name(table_name),
            schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            location=location,
        )

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
        if write_disposition in ("append", "skip", "replace"):
            # replace will have triggered the tables to be truncated so we can just append
            table.append(table_data)
        else:
            raise DestinationTerminalException(
                "Unsupported write disposition {write_disposition} for pyiceberg destination."
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
