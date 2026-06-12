"""Support for ingesting data from an SQL database."""

import logging
from abc import abstractmethod
from typing import Generator, Iterator, NamedTuple, Optional

import pyarrow as pa
import sqlalchemy as sa
from pydantic import SecretStr
from pydantic_settings import BaseSettings

from elt_common.extract import ResourceProperties, ResourceWriteProperties, Watermark, BaseExtract

LOGGER = logging.getLogger(__name__)


class SqlDatabaseSourceConfig(BaseSettings):
    """Configuration required to connect to a database"""

    # connection
    drivername: str
    database: str
    database_schema: Optional[str] = None
    port: Optional[int] = None
    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None

    # loading behaviour
    chunk_size: int = 5000

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
    """

    write_properties: Optional[ResourceWriteProperties] = None
    watermark_column: Optional[str] = None


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

        LOGGER.debug(
            f"Creating engine for {config.drivername} database at "
            f"{config.host}:{config.port}/{config.database}"
        )
        self._engine = sa.create_engine(config.connection_url)
        self._metadata = sa.MetaData(schema=config.database_schema)

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

            def extractor(watermark):
                return self._extract_table(name, watermark=watermark, conn=conn)

            properties = ResourceProperties(
                extractor=extractor,
                write_properties=write_properties,
                watermark_column=watermark_column,
            )

            yield name, properties

    def _extract_table(
        self,
        name: str,
        *,
        conn: sa.Connection,
        watermark: Watermark | None = None,
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

        result = conn.execution_options(yield_per=self._chunk_size).execute(query)
        for partition in result.mappings().partitions():
            yield pa.Table.from_pylist(partition)
