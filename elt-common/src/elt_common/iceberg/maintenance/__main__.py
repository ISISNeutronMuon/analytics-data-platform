"""A standalone module to perform Iceberg table maintenance tasks.
See https://iceberg.apache.org/docs/latest/maintenance/ for more details.

Use the Trino query engine over pyiceberg as the latter currently only supports
the `expire_snapshots` operation.
"""

import contextlib
import dataclasses
import logging
import os
from typing import Sequence

import click
import humanize
import pendulum
from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.sql.expression import text
from sqlalchemy.exc import ProgrammingError as SqlProgrammingError
from trino.auth import BasicAuthentication

ENV_PREFIX = "ELT_COMMON__ICEBERG_MAINT_TRINO__"
LOG_FORMAT = "%(asctime)s:%(module)s:%(levelname)s:%(message)s"
LOG_FORMAT_DATE = "%Y-%m-%dT%H:%M:%S"

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
    def from_env(cls) -> "TrinoCredentials":
        def _get_env_or_raise(field: str):
            try:
                return os.environ[f"{ENV_PREFIX}{field.upper()}"]
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
            results = conn.execute(text(stmt)).fetchall()

        finished_at = pendulum.now()
        LOGGER.debug(f"Completed in {humanize.precisedelta(finished_at - started_at)}")
        LOGGER.debug(f"Returned {results}")
        return results

    def list_iceberg_tables(
        self, ignore_namespaces: Sequence[str] = ("information_schema", "system")
    ) -> Sequence[str]:
        """List all iceberg tables in the catalog. Names are returned fully qualified with the namespace name."""

        def _is_iceberg_table(namespace: str, table_name: str) -> bool:
            # A '{table_id}$properties' table must exist if it is an iceberg table. If not it is probably a view
            try:
                self.execute(f'describe {namespace}."{table_name}$properties"', conn)
            except SqlProgrammingError:
                LOGGER.debug(f"{namespace}.{table_name} is not an iceberg table.")
                return False

            return True

        with self.engine.connect() as conn:
            namespaces = [
                row[0]
                for row in self.execute("show schemas", conn)
                if row[0] not in ignore_namespaces
            ]
            table_identifiers = []
            for ns in namespaces:
                table_identifiers.extend(
                    [
                        f"{ns}.{row[0]}"
                        for row in self.execute(f"show tables in {ns}", conn)
                        if _is_iceberg_table(ns, row[0])
                    ]
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


class IcebergTableMaintenaceSql:
    def __init__(self, query_engine: TrinoQueryEngine):
        self._query_engine = query_engine

    def run(self, table_identifiers: Sequence[str]):
        """Run Iceberg maintenance operations

        By default runs (sequentially):

          - expire_snapshots
          - optimize_manifests
          - optimize
          - remove_orphan_files

        See https://trino.io/docs/current/connector/iceberg.html#alter-table-execute

        :param table_identifiers: Run operations on this list of table identifiers
                                  ("namespace.tablename").
        """
        commands = ("expire_snapshots", "optimize_manifests", "optimize", "remove_orphan_files")
        for table_id in table_identifiers:
            self._run_alter_table_execute(table_id, commands)

    def _run_alter_table_execute(self, table_identifier: str, commands: Sequence[str]):
        """Run a a list of 'alter table {} execute {}' statments on the given table"""

        def _sql_stmt(cmd: str) -> str:
            return f"alter table {table_identifier} execute {cmd}"

        with self._query_engine.engine.connect() as conn:
            for cmd in commands:
                self._query_engine.execute(_sql_stmt(cmd), connection=conn)


@click.command()
@click.option("-t", "--table", multiple=True)
@click.option("-l", "--log-level", default="INFO")
def cli(table: Sequence[str], log_level: str):
    """Launch the maintenance tasks from the command line.
    By default all namespaces and tables are examined."""
    logging.basicConfig(
        format=LOG_FORMAT,
        datefmt=LOG_FORMAT_DATE,
    )
    LOGGER.setLevel(log_level)
    trino = TrinoQueryEngine(TrinoCredentials.from_env())
    iceberg_maintenance = IcebergTableMaintenaceSql(trino)
    if not table:
        table = trino.list_iceberg_tables()
    iceberg_maintenance.run(table)


if __name__ == "__main__":
    cli()
