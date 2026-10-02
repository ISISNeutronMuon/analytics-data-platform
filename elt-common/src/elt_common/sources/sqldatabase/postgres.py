"""Provide any specifics for Postgres databases"""

from . import SqlDatabaseSourceConfig


class PostgresSourceConfig(SqlDatabaseSourceConfig):
    drivername: str = "postgresql+psycopg"
