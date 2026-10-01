from elt_common.sources.sqldatabase import SqlDatabaseExtract
from elt_common.sources.sqldatabase.postgres import PostgresSourceConfig


class Extract(SqlDatabaseExtract[PostgresSourceConfig]):
    pass
