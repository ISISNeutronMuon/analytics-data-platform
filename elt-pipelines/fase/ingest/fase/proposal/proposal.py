from elt_common.extract import (
    ResourceWriteProperties,
    Watermark,  # noqa: F401
)
from elt_common.sources.sqldatabase import (
    SqlDatabaseExtract,
    SqlDatabaseSourceConfig,
    TableInfo,
)


class PipelinePostgresConfig(SqlDatabaseSourceConfig):
    drivername: str = "postgresql+psycopg"
    table: str

    @property
    def target_tables(self) -> list[str]:
        return [t.strip() for t in self.table.split(",") if t.strip()]


class Extract(SqlDatabaseExtract):
    config_cls = PipelinePostgresConfig

    def table_info(self) -> dict[str, TableInfo]:
        """Defines the target tables and their ingestion strategy."""
        return {
            table_name: TableInfo(
                write_properties=ResourceWriteProperties(write_mode="replace")
            )
            for table_name in self.config.target_tables
        }
