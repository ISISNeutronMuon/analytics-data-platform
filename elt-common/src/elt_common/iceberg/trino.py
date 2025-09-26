import contextlib
import dataclasses
import logging
import os
from typing import Sequence

import humanize
import pendulum
from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.sql.expression import text
from sqlalchemy.exc import ProgrammingError as SqlProgrammingError
from trino.auth import BasicAuthentication

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class TrinoCredentials:
    host: str
    port: str
    catalog: str
    user: str
    password: str
    http_scheme: str = "https"

    @classmethod
    def from_env(cls, env_prefix: str) -> "TrinoCredentials":
        def _get_env_or_raise(field: str):
            try:
                return os.environ[f"{env_prefix}{field.upper()}"]
            except KeyError as exc:
                raise KeyError(f"Missing required environment variable: {str(exc)}") from exc

        kwargs = {field.name: _get_env_or_raise(field.name) for field in dataclasses.fields(cls)}
        return TrinoCredentials(**kwargs)


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
        LOGGER.debug(f"Executing SQL '{stmt}'")
        started_at = pendulum.now()

        if connection:
            context_mgr = contextlib.nullcontext(connection)
        else:
            context_mgr = self.engine.connect()

        with context_mgr as conn:
            result = conn.execute(text(stmt))
            rows = result.fetchall() if result.returns_rows else None

        finished_at = pendulum.now()
        LOGGER.debug(f"Completed in {humanize.precisedelta(finished_at - started_at)}")
        LOGGER.debug(f"Returned {rows}")
        return rows

    def list_iceberg_tables_for_maintenance(
        self,
        ignore_namespaces: Sequence[str] = ("information_schema", "system"),
    ) -> Sequence[str]:
        """List all iceberg tables in the catalog. Names are returned fully qualified with the namespace name."""

        def _is_iceberg_needing_maintenance(namespace: str, table_name: str) -> bool:
            # An Iceberg table requiring maintenance must have a '{table_id}$properties' and have a valid
            # current snapshot
            props_table = f'{namespace}."{table_name}$properties"'
            try:
                self.execute(f"describe {props_table}", conn)
            except SqlProgrammingError:
                LOGGER.debug(
                    f"{props_table} does not exist. Assuming {namespace}.{table_name} is not an iceberg table."
                )
                return False

            rows = self.execute(
                f"select value from {props_table} where key = 'current-snapshot-id'", conn
            )
            return rows[0][0] != "none"

        LOGGER.info("Querying catalog for Iceberg tables")
        with self.engine.connect() as conn:
            namespaces = map(
                lambda row: row[0],
                filter(
                    lambda row: row[0] not in ignore_namespaces, self.execute("show schemas", conn)
                ),
            )
            table_identifiers = []
            for ns in namespaces:
                table_identifiers.extend(
                    map(
                        lambda row: f"{ns}.{row[0]}",
                        filter(
                            lambda row: _is_iceberg_needing_maintenance(ns, row[0]),
                            self.execute(f"show tables in {ns}", conn),
                        ),
                    )
                )

        return table_identifiers

    # private
    def _create_engine(self, credentials: TrinoCredentials) -> Engine:
        return create_engine(
            self.url,
            connect_args={
                "auth": BasicAuthentication(credentials.user, credentials.password),
                "http_scheme": credentials.http_scheme,
            },
        )
