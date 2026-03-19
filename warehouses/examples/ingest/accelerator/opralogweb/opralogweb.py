"""Pull data from the Opralogweb database"""

import logging
from typing import Iterator, Tuple

import pyarrow as pa
import sqlalchemy as sa
from pydantic_settings import BaseSettings, SettingsConfigDict


class SourceConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="opralogweb__")

    drivername: str
    database: str
    database_schema: str
    port: int
    host: str
    username: str
    password: str
    tables: list[str]


def extract(
    source_config: SourceConfig,
    *,
    backfill: bool = False,
    chunk_size: int = 5000,
) -> Iterator[Tuple[str, pa.Table]]:
    """Pull data from each configured Opralogweb table."""
    connection_url = sa.URL.create(
        drivername=source_config.drivername,
        username=source_config.username,
        password=source_config.password,
        host=source_config.host,
        port=source_config.port,
        database=source_config.database,
    )
    logging.debug(
        "Connecting to Opralogweb database at %s:%s/%s",
        source_config.host,
        source_config.port,
        source_config.database,
    )

    engine = sa.create_engine(connection_url)
    metadata = sa.MetaData(schema=source_config.database_schema)

    with engine.connect() as conn:
        for table_name in source_config.tables:
            logging.debug("Extracting table %s", table_name)

            table = sa.Table(
                table_name,
                metadata,
                autoload_with=engine,
            )
            result = conn.execution_options(yield_per=chunk_size).execute(
                sa.select(table)
            )
            for partition in result.mappings().partitions():
                yield table_name, pa.Table.from_pylist(list(partition))
