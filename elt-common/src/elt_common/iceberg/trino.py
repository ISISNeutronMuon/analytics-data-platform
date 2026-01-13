import contextlib
import dataclasses
import logging
import os
import re
from typing import Sequence

import humanize
import pendulum
from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.expression import text
from trino.auth import BasicAuthentication

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class TrinoCredentials:
    host: str
    port: str
    catalog: str
    user: str | None
    password: str | None
    http_scheme: str = "https"

    @classmethod
    def from_env(cls, env_prefix: str) -> "TrinoCredentials":
        def _get_env(field: dataclasses.Field):
            key = f"{env_prefix}{field.name.upper()}"
            val = os.getenv(key)
            if val is not None:
                return val
            elif field.default is not dataclasses.MISSING:
                return field.default
            elif getattr(field, "default_factory", dataclasses.MISSING) is not dataclasses.MISSING:
                return field.default_factory()  # type: ignore
            else:
                raise KeyError(f"Missing required environment variable: {key}")

        kwargs = {f.name: _get_env(f) for f in dataclasses.fields(cls)}
        return cls(**kwargs)


class TrinoQueryEngine:
    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def url(self) -> str:
        return self._url

    def __init__(self, credentials: TrinoCredentials):
        """Initlialize an object and create an Engine"""
        self._url = f"trino://{credentials.host}:{credentials.port}/{credentials.catalog}"
        self._engine = self._create_engine(credentials)

    def execute(self, stmt: str, connection: Connection | None = None):
        """Execute a SQL statement and return the results.
        Supply an optional connection to avoid one being created for multiple queries in quick succession"""
        started_at = pendulum.now()

        if connection:
            context_mgr = contextlib.nullcontext(connection)
        else:
            context_mgr = self.engine.connect()

        with context_mgr as conn:
            try:
                result = conn.execute(text(stmt))
            except ProgrammingError as exc:
                raise ValueError(str(exc)) from exc

            rows = result.fetchall() if result.returns_rows else None

        finished_at = pendulum.now()
        LOGGER.debug(
            f"Completed in {humanize.precisedelta(finished_at - started_at)}; rows={len(rows) if rows else 0}"
        )
        return rows

    def list_iceberg_tables(self) -> Sequence[str]:
        """List all iceberg tables in the catalog. Names are returned fully qualified with the namespace name."""
        LOGGER.info("Querying catalog for Iceberg tables")
        with self.engine.connect() as conn:
            rows = self.execute("select * from system.iceberg_tables", connection=conn)
        if not rows:
            return []

        return [f"{row[0]}.{row[1]}" for row in rows]

    @classmethod
    def validate_table_identifier(cls, table_identifier: str):
        """Validate table_identifier format (schema.table)"""
        if not re.match(r"^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$", table_identifier):
            raise ValueError(f"Invalid table identifier: {table_identifier}")

    @classmethod
    def validate_retention_threshold(cls, retention_threshold: str):
        """Validate retention_threshold format (e.g., 7d, 24h)"""
        if not re.match(r"^\d+[dhms]$", retention_threshold):
            raise ValueError(f"Invalid retention threshold format: {retention_threshold}")

    # private
    def _create_engine(self, credentials: TrinoCredentials) -> Engine:
        if credentials.user is None or credentials.password is None:
            auth = BasicAuthentication("trino", "")
        else:
            auth = BasicAuthentication(credentials.user, credentials.password)

        return create_engine(
            self.url,
            connect_args={
                "auth": auth,
                "http_scheme": credentials.http_scheme,
                "verify": False,
            },
        )
