import logging
from typing import Dict

import pyarrow as pa
from pydantic_settings import SettingsConfigDict

from elt_common.sources.sqldatabase import SqlDatabaseExtract, SqlDatabaseSourceConfig

LOGGER = logging.getLogger(__name__)

# Type mapping between Postgres SQLAlchemy types and PyArrow
DEFAULT_TYPE_MAP: Dict[str, str] = {
    "int": "bigint",
    "bool": "bool",
    "json": "text",
    "uuid": "text",
    "float": "double",
    "numeric": "double",
    "double": "double",
    "timestamp": "timestamp",
    "date": "date",
}

PA_TYPE_MAPPING = {
    "bigint": pa.int64(),
    "bool": pa.bool_(),
    "double": pa.float64(),
    "timestamp": pa.timestamp("us", tz="UTC"),  # Must match Iceberg timestamptz
    "date": pa.date32(),
    "text": pa.string(),
}


class PostgresConfig(SqlDatabaseSourceConfig):
    model_config = SettingsConfigDict(
        extra="ignore",
        protected_namespaces=(),
    )

    type_map: Dict[str, str] = DEFAULT_TYPE_MAP


class PostgresExtract(SqlDatabaseExtract):
    config_cls = PostgresConfig

    def map_sql_to_pq_type(self, sql_type: Any) -> pa.DataType:  # noqa: F821
        t = str(sql_type).lower()
        if "timestamp" in t:
            # Postgres needs UTC timestamp for Iceberg compatibility
            return pa.timestamp("us", tz="UTC")

        type_map = getattr(self.config, "type_map", DEFAULT_TYPE_MAP)
        for keyword, mapped_type in type_map.items():
            if keyword in t:
                return PA_TYPE_MAPPING.get(mapped_type, pa.string())
        return pa.string()
