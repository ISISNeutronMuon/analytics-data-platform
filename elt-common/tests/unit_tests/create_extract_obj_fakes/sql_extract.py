from typing import Optional

from elt_common.sources.sqldatabase import SqlDatabaseExtract, TableInfo


class Extract(SqlDatabaseExtract):
    def table_info(self) -> dict[str, Optional[TableInfo]]:
        return {}
