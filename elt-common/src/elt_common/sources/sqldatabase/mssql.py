"""Provide any specifics for MS SQL databases"""

from . import SqlDatabaseSourceConfig


class MSSQLSourceConfig(SqlDatabaseSourceConfig):
    drivername: str = "mssql+pymssql"
