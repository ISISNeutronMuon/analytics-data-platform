from typing import Optional

from elt_common.sources.sqldatabase import SqlDatabaseExtract, SqlDatabaseSourceConfig, TableInfo


class CustomSqlConfig(SqlDatabaseSourceConfig):
    database: str = "hardcoded_dbname"


class Extract(SqlDatabaseExtract):
    config_cls = CustomSqlConfig

    def table_info(self) -> dict[str, Optional[TableInfo]]:
        return {}
