"""Pull data from the Opralogweb database"""

import logging

from elt_common.sources.sqldatabase import DatabaseSourceConfig
from elt_common.typing import DataItems, TableItems
import pyarrow as pa
import sqlalchemy as sa


class Extract:
    source_config_cls = DatabaseSourceConfig

    def __init__(self, source_config: DatabaseSourceConfig):
        self._source_config = source_config

        logging.debug(
            f"Creating engine for {source_config.drivername} database at "
            f"{source_config.host}:{source_config.port}/{source_config.database}"
        )
        self._engine = sa.create_engine(source_config.connection_url)
        self._metadata = sa.MetaData(schema=source_config.database_schema)

    def __call__(self, table_info: TableItems) -> DataItems:
        with self._engine.connect() as conn:
            for name, info in table_info.items():
                logging.debug(
                    f"Extracting table {name} in chunks of {self._source_config.chunk_size} rows."
                )

                table = sa.Table(
                    info.name,
                    self._metadata,
                    autoload_with=self._engine,
                )
                query = sa.select(table)
                if (cursor := info.cursor_column) is not None and (
                    cursor.max_value is not None
                ):
                    logging.debug(
                        f"Cursor value detected. Limiting query to {cursor.column} > {cursor.max_value}"
                    )
                    query = query.where(sa.column(cursor.column) > cursor.max_value)

                result = conn.execution_options(
                    yield_per=self._source_config.chunk_size
                ).execute(query)
                for partition in result.mappings().partitions():
                    yield info.name, pa.Table.from_pylist(list(partition))
