"""Support for extracting data from a SQL database"""

from typing import Optional

from pydantic import SecretStr
from pydantic_settings import BaseSettings
import sqlalchemy as sa


class DatabaseSourceConfig(BaseSettings):
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
