"""Support for extracting data from a SQL database"""

import logging
from typing import Optional

from elt_common.typing import DataChunks
import pyarrow as pa
from pydantic import SecretStr
from pydantic_settings import BaseSettings
import sqlalchemy as sa


LOGGER = logging.getLogger(__name__)


class SqlDatabaseSourceConfig(BaseSettings):
    """Capture configuration required to connect to a database"""

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
            password=self.password.get_secret_value() if self.password is not None else None,
            host=self.host,
            port=self.port,
            database=self.database,
        )


def sql_database(source_config: SqlDatabaseSourceConfig, tables: list[str]) -> DataChunks:
    """Return data from tables defined by the configured SQL database

    :param source_config: Define the connection parameters for the database
    :param tables: If provided a list of tables to load, else all tables from the schema are loaded.
    """
    LOGGER.debug(
        f"Creating engine for {source_config.drivername} database at "
        f"{source_config.host}:{source_config.port}/{source_config.database}"
    )
    engine = sa.create_engine(source_config.connection_url)
    metadata = sa.MetaData(schema=source_config.database_schema)
    with engine.connect() as conn:
        for name in tables:
            yield from _sql_table(name, source_config.chunk_size, conn, metadata)


def _sql_table(
    name: str, chunk_size: int, conn: sa.Connection, metadata: sa.MetaData
) -> DataChunks:
    """Extract table data from the connected datbase

    :param name: Table name
    :param chunk_size: Extracts the table in chunks of this number of rows
    :param conn: Connection to a database/schema
    """
    LOGGER.debug(f"Extracting table {name} in chunks of {chunk_size} rows.")
    table = sa.Table(
        name,
        metadata,
        autoload_with=conn.engine,
    )
    query = sa.select(table)
    query_mutator = _noop
    result = conn.execution_options(yield_per=chunk_size).execute(query_mutator(table, query))
    for partition in result.mappings().partitions():
        yield pa.Table.from_pylist(list(partition))  # type: ignore


def _noop(_, query):
    """Default mutator if no query mutation function provided"""
    return query


# class SqlDatabaseExtract:
#     source_config_cls = SqlDatabaseSourceConfig

#     def __init__(self, source_config: SqlDatabaseSourceConfig):
#         self._source_config = source_config

#         LOGGER.debug(
#             f"Creating engine for {source_config.drivername} database at "
#             f"{source_config.host}:{source_config.port}/{source_config.database}"
#         )
#         self._engine = sa.create_engine(source_config.connection_url)
#         self._metadata = sa.MetaData(schema=source_config.database_schema)

#     @property
#     def chunk_size(self):
#         return self._source_config.chunk_size

#     @abstractmethod
#     def tables(self):
#         raise NotImplementedError(
#             "Subclass should implement this to provide details of tables to be extracted."
#         )

#     def extract(self, watermarks: WatermarkInfo) -> DataItems:
#         """Yield the results of extracting the named Tables from the source."""
#         table_info = self.tables()
#         with self._engine.connect() as conn:
#             for name in table_info.keys():
#                 yield from self.extract_single(conn, name, watermark=watermarks.get(name))

#     def extract_single(
#         self,
#         conn: sa.Connection,
#         name: str,
#         *,
#         watermark: Watermark | None = None,
#         query_mutator=None,
#     ) -> DataItems:
#         LOGGER.debug(f"Extracting table {name} in chunks of {self.chunk_size} rows.")
#         table = sa.Table(
#             name,
#             self._metadata,
#             autoload_with=self._engine,
#         )
#         query = sa.select(table)
#         if watermark is not None:
#             column, max_value = watermark.column, watermark.value
#             LOGGER.debug(f"Cursor value detected. Limiting query to {column} > {max_value}")
#             query = query.where(sa.column(column) > max_value)  # type: ignore

#         if query_mutator is None:
#             query_mutator = _noop
#         result = conn.execution_options(yield_per=self.chunk_size).execute(
#             query_mutator(table, query)
#         )
#         for partition in result.mappings().partitions():
#             yield name, pa.Table.from_pylist(list(partition))  # type: ignore
