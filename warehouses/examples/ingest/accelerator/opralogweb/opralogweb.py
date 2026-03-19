"""Pull data from the Opralogweb database"""

import logging
from typing import Optional, Iterator, Tuple

import pyarrow as pa
from pydantic import SecretStr
from pydantic_settings import BaseSettings
import sqlalchemy as sa


class DatabaseSourceConfig(BaseSettings):
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


class Extract:
    source_config_cls = DatabaseSourceConfig

    def __init__(self, source_config: DatabaseSourceConfig):
        self._source_config = source_config
        connection_url = sa.URL.create(
            drivername=source_config.drivername,
            username=source_config.username,
            password=source_config.password.get_secret_value()
            if source_config.password is not None
            else None,
            host=source_config.host,
            port=source_config.port,
            database=source_config.database,
        )
        logging.debug(
            f"Creating engine for {source_config.drivername} database at "
            f"{source_config.host}:{source_config.port}/{source_config.database}"
        )
        self._engine = sa.create_engine(connection_url)
        self._metadata = sa.MetaData(schema=source_config.database_schema)

    def __call__(self, table_info) -> Iterator[Tuple[str, pa.Table]]:
        with self._engine.connect() as conn:
            for name, info in table_info.items():
                logging.debug(
                    f"Extracting table {name} in chunks of {self._source_config.chunk_size} rows."
                )

                table = sa.Table(
                    name,
                    self._metadata,
                    autoload_with=self._engine,
                )
                query = sa.select(table)
                if (cursor_value := info["cursor_value"]) is not None:
                    logging.debug(
                        f"Cursor value detected. Limiting query to > {cursor_value}"
                    )
                    query = query.where(
                        sa.column(info["config"].cursor_column) > cursor_value
                    )

                result = conn.execution_options(
                    yield_per=self._source_config.chunk_size
                ).execute(query)
                for partition in result.mappings().partitions():
                    yield table.name, pa.Table.from_pylist(list(partition))
