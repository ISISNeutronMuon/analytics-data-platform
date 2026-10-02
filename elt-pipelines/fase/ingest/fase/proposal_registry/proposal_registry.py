from elt_common.sources.sqldatabase import SqlDatabaseExtract
from elt_common.sources.sqldatabase.oracle import OracleSourceConfig


class Extract(SqlDatabaseExtract):
    config_cls = OracleSourceConfig
