"""Support for extracting data from a SQL database"""

import logging
from typing import Optional

from elt_common.typing import DataItems, TableItems
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


class SqlDatabaseExtract:
    source_config_cls = SqlDatabaseSourceConfig

    def __init__(self, source_config: SqlDatabaseSourceConfig):
        self._source_config = source_config

        LOGGER.debug(
            f"Creating engine for {source_config.drivername} database at "
            f"{source_config.host}:{source_config.port}/{source_config.database}"
        )
        self._engine = sa.create_engine(source_config.connection_url)
        self._metadata = sa.MetaData(schema=source_config.database_schema)

    def __call__(self, table_info: TableItems) -> DataItems:
        with self._engine.connect() as conn:
            for name, info in table_info.items():
                LOGGER.debug(
                    f"Extracting table {name} in chunks of {self._source_config.chunk_size} rows."
                )

                table = sa.Table(
                    info.name,
                    self._metadata,
                    autoload_with=self._engine,
                )
                query = sa.select(table)
                if (cursor := info.cursor_column) is not None and (cursor.max_value is not None):
                    LOGGER.debug(
                        f"Cursor value detected. Limiting query to {cursor.column} > {cursor.max_value}"
                    )
                    query = query.where(sa.column(cursor.column) > cursor.max_value)

                result = conn.execution_options(yield_per=self._source_config.chunk_size).execute(
                    query
                )
                for partition in result.mappings().partitions():
                    yield info.name, pa.Table.from_pylist(list(partition))
