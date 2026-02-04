import contextlib
import logging
import re
from typing import Sequence

import humanize
import pendulum
from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.expression import text
from trino.auth import BasicAuthentication

LOGGER = logging.getLogger(__name__)


class TrinoQueryEngine:
    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def url(self) -> str:
        return self._url

    def __init__(
        self,
        host: str,
        port: str,
        catalog: str,
        user: str,
        password: str,
        http_scheme="https",
        verify=True,
    ):
        """Initlialize an object and create an Engine"""
        self._url = f"trino://{host}:{port}/{catalog}"
        self._engine = self._create_engine(user, password, http_scheme=http_scheme, verify=verify)

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
    def _create_engine(self, user: str, password: str, **connect_args) -> Engine:
        if user is None or password is None:
            auth = BasicAuthentication("trino", "")
        else:
            auth = BasicAuthentication(user, password)

        return create_engine(
            self.url,
            connect_args=dict(auth=auth, **connect_args),
        )
