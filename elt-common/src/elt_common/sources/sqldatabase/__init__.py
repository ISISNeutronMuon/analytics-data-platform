"""Support for ingesting data from an SQL database."""

import json
import logging
from abc import abstractmethod
from typing import Callable, Generator, Iterator, NamedTuple, Optional

import pyarrow as pa
import sqlalchemy as sa
from pydantic import PositiveInt, SecretStr
from pydantic_settings import BaseSettings
from sqlalchemy import Select

from elt_common.extract import BaseExtract, ResourceProperties, ResourceWriteProperties, Watermark
from elt_common.sources.sqldatabase.schema import to_pyarrow_schema

LOGGER = logging.getLogger(__name__)

DEFAULT_PA_TYPE_MAPPING = {
    "bigint": pa.int64(),
    "bool": pa.bool_(),
    "double": pa.float64(),
    "timestamp": pa.timestamp("us"),
    "date": pa.date32(),
    "text": pa.string(),
}


class SqlDatabaseSourceConfig(BaseSettings):
    drivername: str
    database: str
    database_schema: Optional[str] = None
    port: Optional[int] = None
    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None

    chunk_size: int = 5000
    """If the query returns more than chunk_size rows, fetch them in multiple chunks of at most this size"""

    row_limit: Optional[PositiveInt] = None
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

    write_properties: Optional[ResourceWriteProperties] = None
    watermark_column: Optional[str] = None
    destination_table_name: Optional[str] = None


class SqlDatabaseExtract(BaseExtract):
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
        self._chunk_size = config.chunk_size
        self._row_limit = config.row_limit

        LOGGER.debug(
            f"Creating engine for {config.drivername} database at "
            f"{config.host}:{config.port}/{config.database}"
        )
        self._engine = sa.create_engine(config.connection_url)
        self._metadata = sa.MetaData(schema=config.database_schema)

    def get_table_schema(self, table_name: str) -> pa.Schema:
        """Autoloads the SQL table and constructs a PyArrow schema using schema.py."""
        table = sa.Table(
            table_name,
            self._metadata,
            autoload_with=self._engine,
        )
        schema = to_pyarrow_schema(table)

        # Apply column normalization (e.g. lowercase) if overridden by subclasses
        normalized_fields = [
            pa.field(self.normalize_column_name(field.name), field.type, field.nullable)
            for field in schema
        ]
        return pa.schema(normalized_fields)

    def normalize_column_name(self, name: str) -> str:
        return name

    @abstractmethod
    def table_info(self) -> dict[str, Optional[TableInfo]]:
        """Define the tables to be extracted from the DB.

        Each key in the returned dict is a table name. Their values can include
        extra properties for controlling ingestion, see :class:`TableInfo`.
        """
        pass

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

            def extractor(watermark, *, _name=name):
                return self._extract_table(_name, watermark=watermark, conn=conn)

            properties = ResourceProperties(
                extractor=extractor,
                write_properties=write_properties,
                watermark_column=watermark_column,
            )

            destination_table = name
            if table_props is not None and table_props.destination_table_name is not None:
                destination_table = table_props.destination_table_name

            yield destination_table, properties

    def _extract_table(
        self,
        name: str,
        *,
        conn: sa.Connection,
        watermark: Watermark | None = None,
        query_filter: Callable[[Select], Select] | None = None,
    ) -> Iterator[pa.Table]:
        LOGGER.debug(f"Extracting table {name} in chunks of {self._chunk_size} rows.")
        table = sa.Table(
            name,
            self._metadata,
            autoload_with=self._engine,
        )
        query = sa.select(table)
        if watermark is not None:
            column, max_value = watermark.column, watermark.value
            LOGGER.debug(f"Cursor value detected. Limiting query to {column} > {max_value}")
            query = query.where(sa.column(column) > max_value)

        if query_filter:
            query = query_filter(query)

        query = query.limit(self._row_limit)

        result = conn.execution_options(yield_per=self._chunk_size).execute(query)

        target_schema = self.get_table_schema(name)

        column_names = list(result.keys())

        has_data = False
        while True:
            rows = result.fetchmany(self._chunk_size)

            if not rows:
                break

            has_data = True

            # Convert SQLAlchemy Row objects to column arrays
            columns = {}

            for idx, column_name in enumerate(column_names):
                columns[column_name] = [row[idx] for row in rows]

            arrow_arrays = []

            for field in target_schema:
                values = columns.get(
                    field.name,
                    [None] * len(rows),
                )

                # JSON / JSONB -> string for Iceberg
                if pa.types.is_string(field.type):
                    values = [
                        json.dumps(v)
                        if isinstance(v, (dict, list))
                        else str(v)
                        if v is not None and not isinstance(v, str)
                        else v
                        for v in values
                    ]
                elif pa.types.is_integer(field.type):
                    values = [int(v) if v is not None else None for v in values]
                elif pa.types.is_floating(field.type):
                    values = [float(v) if v is not None else None for v in values]

                array = pa.array(
                    values,
                    type=field.type,
                )
                arrow_arrays.append(array)

            yield pa.Table.from_arrays(
                arrow_arrays,
                schema=target_schema,
            )

        # Return empty table with schema when no rows
        if not has_data:
            empty_arrays = [pa.array([], type=field.type) for field in target_schema]

            yield pa.Table.from_arrays(
                empty_arrays,
                schema=target_schema,
            )
