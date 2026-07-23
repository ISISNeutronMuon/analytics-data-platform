from elt_common.extract import ResourceWriteProperties
from elt_common.sources.sqldatabase import SqlDatabaseSourceConfig, TableInfo
from fase.utils.postgres import PostgresExtract


class PipelinePostgresConfig(SqlDatabaseSourceConfig):
    model_config = {
        "env_prefix": "proposal__",
        "env_nested_delimiter": "__",
        "extra": "ignore",
        "protected_namespaces": (),
    }

    drivername: str = "postgresql+psycopg2"
    table: str

    @property
    def target_tables(self) -> list[str]:
        return [t.strip() for t in self.table.split(",") if t.strip()]


class Extract(PostgresExtract):
    config_cls = PipelinePostgresConfig

    def table_info(self) -> dict[str, TableInfo]:
        """Defines the target tables and their ingestion strategy."""
        return {
            table_name: TableInfo(
                write_properties=ResourceWriteProperties(write_mode="replace")
            )
            for table_name in self.config.target_tables
        }
