"""Provide any specifics for Oracle databases"""

import sqlalchemy as sa

from . import SqlDatabaseSourceConfig


class OracleSourceConfig(SqlDatabaseSourceConfig):
    drivername: str = "oracle+oracledb"

    @property
    def connection_url(self) -> sa.URL:
        return sa.URL.create(
            drivername=self.drivername,
            username=self.username,
            password=self.password.get_secret_value() if self.password else None,
            host=self.host,
            port=self.port,
            query={"service_name": self.database},
        )
