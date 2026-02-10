import logging
from typing import Sequence

import click
from elt_common.iceberg.trino import TrinoQueryEngine
from pydantic_settings import BaseSettings, SettingsConfigDict


ENV_PREFIX = "ELT_COMMON_ICEBERG_MAINT_TRINO__"
LOG_FORMAT = "%(asctime)s:%(module)s:%(levelname)s:%(message)s"
LOG_FORMAT_DATE = "%Y-%m-%dT%H:%M:%S"

LOGGER = logging.getLogger(__name__)


class TrinoCredentials(BaseSettings):
    model_config = SettingsConfigDict(env_prefix=ENV_PREFIX)

    host: str
    port: str
    catalog: str
    user: str | None
    password: str | None
    http_scheme: str = "https"
    verify: bool = True


class IcebergTableMaintenaceSql:
    """See https://trino.io/docs/current/connector/iceberg.html#alter-table-execute"""

    def __init__(self, query_engine: TrinoQueryEngine):
        self._query_engine = query_engine

    def expire_snapshots(self, table_identifier: str, retention_threshold: str):
        """Expire snapshots older than the given threshold"""
        self._query_engine.validate_retention_threshold(retention_threshold)
        self._run_alter_table_execute(
            table_identifier, f"expire_snapshots(retention_threshold => '{retention_threshold}')"
        )

    def optimize_manifests(self, table_identifier: str):
        self._run_alter_table_execute(table_identifier, "optimize_manifests")

    def optimize(self, table_identifier: str):
        self._run_alter_table_execute(table_identifier, "optimize")

    def remove_orphan_files(self, table_identifier: str, retention_threshold: str):
        self._query_engine.validate_retention_threshold(retention_threshold)
        self._run_alter_table_execute(
            table_identifier, f"remove_orphan_files(retention_threshold => '{retention_threshold}')"
        )

    def _run_alter_table_execute(self, table_identifier: str, cmd: str):
        """Run 'alter table {} execute {}' statments on the given table"""

        def _sql_stmt() -> str:
            return f"alter table {table_identifier} execute {cmd}"

        self._query_engine.validate_table_identifier(table_identifier)
        LOGGER.debug(f"Executing maintenance command '{cmd}' on table '{table_identifier}'.")
        with self._query_engine.engine.connect() as conn:
            self._query_engine.execute(_sql_stmt(), connection=conn)


@click.command()
@click.option("-t", "--table", multiple=True)
@click.option("-r", "--retention-threshold", default="7d")
@click.option("-l", "--log-level", default="INFO")
def cli(table: Sequence[str], retention_threshold: str, log_level: str):
    """Launch the maintenance tasks from the command line.
    By default all namespaces and tables are examined."""
    try:
        TrinoQueryEngine.validate_retention_threshold(retention_threshold)
    except ValueError as exc:
        raise click.BadParameter(str(exc))

    logging.basicConfig(
        format=LOG_FORMAT,
        datefmt=LOG_FORMAT_DATE,
    )
    LOGGER.setLevel(log_level)

    trino_creds = TrinoCredentials()  # type: ignore
    trino = TrinoQueryEngine(**trino_creds.model_dump(mode="python"))
    iceberg_maintenance = IcebergTableMaintenaceSql(trino)

    if not table:
        table = trino.list_iceberg_tables()
    for table_id in table:
        LOGGER.info(f"Running iceberg maintenance operations on {table_id}")
        try:
            iceberg_maintenance.optimize(table_id)
            iceberg_maintenance.optimize_manifests(table_id)
            iceberg_maintenance.expire_snapshots(table_id, retention_threshold=retention_threshold)
            iceberg_maintenance.remove_orphan_files(
                table_id, retention_threshold=retention_threshold
            )
        except Exception as exc:
            LOGGER.error(
                f"Failed to execute maintenance operation for table '{table_id}': {str(exc)}"
            )
