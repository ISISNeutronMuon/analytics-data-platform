import logging
from typing import Sequence

import click

from elt_common.iceberg.trino import TrinoCredentials, TrinoQueryEngine

ENV_PREFIX = "ELT_COMMON_ICEBERG_MAINT_TRINO_"
LOG_FORMAT = "%(asctime)s:%(module)s:%(levelname)s:%(message)s"
LOG_FORMAT_DATE = "%Y-%m-%dT%H:%M:%S"

LOGGER = logging.getLogger(__name__)


class IcebergTableMaintenaceSql:
    def __init__(self, query_engine: TrinoQueryEngine):
        self._query_engine = query_engine

    def expire_snapshots(self, table_identifier: str, *, retention_threshold: str):
        """Expire snapshots older than the given threshold"""
        self._run_alter_table_execute(
            table_identifier, f"expire_snapshots(retention_threshold => '{retention_threshold}')"
        )

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
        # commands = ("expire_snapshots()", "optimize_manifests", "optimize", "remove_orphan_files")
        # for table_id in table_identifiers:
        #     LOGGER.info(f"Running iceberg maintenance on '{table_id}'")
        #     self._run_alter_table_execute(table_id, cmd=)

    def _run_alter_table_execute(self, table_identifier: str, cmd: str):
        """Run 'alter table {} execute {}' statments on the given table"""

        def _sql_stmt() -> str:
            return f"alter table {table_identifier} execute {cmd}"

        with self._query_engine.engine.connect() as conn:
            self._query_engine.execute(_sql_stmt(), connection=conn)


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
    trino = TrinoQueryEngine(TrinoCredentials.from_env(ENV_PREFIX))
    iceberg_maintenance = IcebergTableMaintenaceSql(trino)
    if not table:
        table = trino.list_iceberg_tables()
    iceberg_maintenance.run(table)
