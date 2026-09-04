"""Support for ingesting data from an SQL database."""

import json
import logging
from abc import abstractmethod
from collections.abc import Callable, Generator, Iterable, Iterator
from typing import NamedTuple

import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa
from pydantic import PositiveInt, SecretStr
from pydantic_settings import BaseSettings
from sqlalchemy import Select

from elt_common.extract import BaseExtract, ResourceProperties, ResourceWriteProperties, Watermark
from elt_common.sources.sqldatabase.schema import to_pyarrow_schema

LOGGER = logging.getLogger(__name__)


def _serialize_json_values(row: dict) -> dict:
    """Ensure dict/list objects in rows are serialized to JSON strings for PyArrow json_ types."""
    return {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in row.items()}


def json_to_str(table: pa.Table) -> pa.Table:
    """Fast-path helper to cast any PyArrow JSON columns in a Table to String columns."""
    # PyArrow JSON extension types are instances of pa.JsonType
    json_field_indices = [
        i for i, field in enumerate(table.schema) if isinstance(field.type, pa.JsonType)
    ]

    if not json_field_indices:
        return table

    new_schema = table.schema
    new_columns = list(table.columns)

    for i in json_field_indices:
        field = table.schema.field(i)
        new_field = field.with_type(pa.string())
        new_schema = new_schema.set(i, new_field)
        new_columns[i] = pc.cast(table.column(i), pa.string())

    return pa.Table.from_arrays(new_columns, schema=new_schema)


def _partition_to_pyarrow_table(partition: Iterable[dict], schema: pa.Schema) -> pa.Table:
    """Converts a partition of mapping rows into a PyArrow Table, handling JSON serialization and schema casting."""
    rows = [_serialize_json_values(row) for row in partition]
    pa_table = pa.Table.from_pylist(rows, schema=schema)
    return json_to_str(pa_table)


class SqlDatabaseSourceConfig(BaseSettings):
    drivername: str
    database: str
    database_schema: str | None = None
    port: int | None = None
    host: str | None = None
    username: str | None = None
    password: SecretStr | None = None

    chunk_size: int = 5000
    """If the query returns more than chunk_size rows, fetch them in multiple chunks of at most this size"""

    row_limit: PositiveInt | None = None
    """Maximum number of rows to return from each table, primarily for testing purposes. No limit if 'None'"""

    @property
    def connection_url(self):
        return sa.URL.create(
            drivername=self.drivername,
            username=self.username,
            password=self.password.get_secret_value() if self.password else None,
            host=self.host,
            port=self.port,
            database=self.database,
        )


class TableInfo(NamedTuple):
    """Extra information for controlling how a table is ingested.

    Each table in a DB can have nondefault write properties, a watermark column,
    both, or neither.

    :ivar write_properties: properties to control how the table is written to the
    destination. If omitted, will default to appending with no partitions or sorting.
    :ivar watermark_column: the column to use for watermarking. If omitted, the
    entire table will be queried on every run
    :ivar destination_table_name: sets the name of the table in Iceberg, if it should
    be different to the name of the DB table
    """

    write_properties: ResourceWriteProperties | None = None
    watermark_column: str | None = None
    destination_table_name: str | None = None


class SqlDatabaseExtract(BaseExtract[SqlDatabaseSourceConfig]):
    """Base class for defining SQL ingest Extract classes.

    Example usage, for an ingest script that reads from 3 tables::

        class Extract(SqlDatabaseExtract):
            def table_info(self):
                return {
                    "a_table": None,
                    "a_table_that_watermarks_ingest_progress": TableInfo(
                        watermark_column="id"
                    ),
                    "a_table_to_replace_entirely_every_time": TableInfo(
                        write_properties=ResourceWriteProperties(
                            write_mode="replace"
                        )
                    )
                }
    """

    config_cls = SqlDatabaseSourceConfig

    def __init__(self, config: SqlDatabaseSourceConfig):
        super().__init__(config)

        LOGGER.debug(
            f"Creating engine for {config.drivername} database at "
            f"{config.host}:{config.port}/{config.database}"
        )
        self._engine = sa.create_engine(config.connection_url)
        self._metadata = sa.MetaData(schema=config.database_schema)

    @abstractmethod
    def table_info(self) -> dict[str, TableInfo | None]:
        """Define the tables to be extracted from the DB.

        Each key in the returned dict is a table name. Their values can include
        extra properties for controlling ingestion, see :class:`TableInfo`.

        This is a convenience method for defining tables whose data can be
        extracted in a straightforward way (all data is extracted, potentially
        limited by a watermark). For tables requiring more complex behaviour
        (e.g. filtering) extend :py:meth:`extract_resource_properties` with
        custom extractors.
        """

    def extract_resource_properties(self):
        """Open a connection to the DB and return ingest properties for tables
        defined by :func:`table_info`.

        The extractor functions yielded as part of this function use the DB
        connection which is only active whilst this function is executing.
        This means the extractors must be called whilst iterating over the
        results of this function.
        """
        with self._engine.connect() as conn:
            yield from self._make_table_properties(conn)

    def _make_table_properties(
        self, conn: sa.Connection
    ) -> Generator[tuple[str, ResourceProperties]]:
        """For each table defined in :func:`table_info`, build a
        :class:`ResourceProperties` which can be used to ingest it"""

        for name, table_props in self.table_info().items():
            write_properties = (
                table_props.write_properties
                if table_props and table_props.write_properties
                else ResourceWriteProperties()
            )
            watermark_column = (
                table_props.watermark_column
                if table_props and table_props.watermark_column
                else None
            )
            resource_name = (
                table_props.destination_table_name
                if table_props and table_props.destination_table_name
                else name
            )

            def extractor(watermark, *, _name=name):
                return self._extract_table(_name, watermark=watermark, conn=conn)

            properties = ResourceProperties(
                extractor=extractor,
                write_properties=write_properties,
                watermark_column=watermark_column,
            )

            yield resource_name, properties

    def _extract_table(
        self,
        name: str,
        *,
        conn: sa.Connection,
        watermark: Watermark | None = None,
        query_filter: Callable[[Select], Select] | None = None,
    ) -> Iterator[pa.Table]:
        LOGGER.debug(f"Extracting table {name} in chunks of {self.config.chunk_size} rows.")
        table = sa.Table(
            name,
            self._metadata,
            autoload_with=self._engine,
        )

        # Apply UTC extraction strictly to Oracle databases to avoid Thin mode DPY-3022 errors
        is_oracle = self._engine.dialect.name == "oracle"

        selected_cols = []
        for col in table.columns:
            if is_oracle:
                col_type_str = str(col.type).upper()
                is_tz = getattr(col.type, "timezone", False) or "WITH TIME ZONE" in col_type_str

                if is_tz:
                    selected_cols.append(sa.func.sys_extract_utc(col).label(col.name))
                else:
                    selected_cols.append(col)
            else:
                selected_cols.append(col)

        query = sa.select(*selected_cols)
        if watermark is not None:
            column, max_value = watermark.column, watermark.value
            LOGGER.debug(f"Cursor value detected. Limiting query to {column} > {max_value}")
            query = query.where(sa.column(column) > max_value)

        if query_filter:
            query = query_filter(query)

        if self.config.row_limit:
            query = query.limit(self.config.row_limit)

        pa_schema = to_pyarrow_schema(table)
        result = conn.execution_options(yield_per=self.config.chunk_size).execute(query)

        has_data = False
        for partition in result.mappings().partitions():
            has_data = True
            yield _partition_to_pyarrow_table(partition, schema=pa_schema)

        if not has_data:
            empty_table = pa.Table.from_batches([], schema=pa_schema)
            yield empty_table
