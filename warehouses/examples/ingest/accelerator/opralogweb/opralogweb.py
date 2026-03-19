"""Pull data from the Opralogweb database"""

import logging
from typing import Optional, Iterator, Sequence, Tuple

import pyarrow as pa
import sqlalchemy as sa
from pydantic_settings import BaseSettings, SettingsConfigDict


class SourceConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="opralogweb__")

    # db connection
    drivername: str
    database: str
    database_schema: Optional[str] = None
    port: Optional[int] = None
    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    # data tables
    chunk_size: int = 5000


def extract(
    source_config: SourceConfig,
    table_names: Sequence[str],
    *,
    backfill: bool = False,
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
        f"Connecting to {source_config.drivername} database at "
        f"{source_config.host}:{source_config.port}/{source_config.database}"
    )

    engine = sa.create_engine(connection_url)
    metadata = sa.MetaData(schema=source_config.database_schema)

    with engine.connect() as conn:
        for table_name in table_names:
            logging.debug(
                f"Extracting table {table_name} in chunks of {source_config.chunk_size} rows"
            )

            table = sa.Table(
                table_name,
                metadata,
                autoload_with=engine,
            )
            result = conn.execution_options(yield_per=source_config.chunk_size).execute(
                sa.select(table)
            )
            for partition in result.mappings().partitions():
                yield table_name, pa.Table.from_pylist(list(partition))
