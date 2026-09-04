import sqlalchemy as sa
from elt_common.extract import ResourceWriteProperties
from elt_common.sources.sqldatabase import (
    SqlDatabaseExtract,
    SqlDatabaseSourceConfig,
    TableInfo,
)


class PipelineOracleConfig(SqlDatabaseSourceConfig):
    drivername: str = "oracle+oracledb"
    database_schema: str = "isisuserdb"  # Source schema of this pipeline
    tables: list[str]

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


class Extract(SqlDatabaseExtract):
    config_cls = PipelineOracleConfig

    def table_info(self) -> dict[str, TableInfo]:
        """Defines the target tables and their ingestion strategy."""
        return {
            table_name: TableInfo(
                write_properties=ResourceWriteProperties(write_mode="replace")
            )
            for table_name in self.config.tables
        }
