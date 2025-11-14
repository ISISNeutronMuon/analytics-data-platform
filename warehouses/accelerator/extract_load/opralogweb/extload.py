from pathlib import Path
import re
import sys
from typing import Sequence, Tuple

import connectorx as cx
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties
import pyiceberg.types as pyt
import pyarrow as pa
import pyarrow.compute as pc
import requests
from sqlalchemy import (
    create_engine,
    make_url,
    inspect,
    Column,
    Engine,
    MetaData,
    Table,
    URL,
)
import sqlalchemy.sql.sqltypes as sqltypes

import os


DATASET_NAME = "src_opralogweb"
LOGBOOK_ENTRY_CHANGES_LAST_SEEN = "logbook_entry_changes_last_seen"
SNAKE_CASE_BREAK_1 = re.compile("([^_])([A-Z][a-z]+)")
SNAKE_CASE_BREAK_2 = re.compile("([a-z0-9])([A-Z])")
SQL_TO_ICEBERG_TYPES = {
    sqltypes.Integer: pyt.LongType,
    sqltypes.INTEGER: pyt.LongType,
    sqltypes.Float: pyt.FloatType,
    sqltypes.FLOAT: pyt.FloatType,
    sqltypes.Double: pyt.DoubleType,
    sqltypes.DOUBLE: pyt.DoubleType,
    sqltypes.DateTime: pyt.TimestampType,
    sqltypes.TIMESTAMP: pyt.TimestampType,
    sqltypes.TIME_TIMEZONE: pyt.TimestamptzType,
    sqltypes.String: pyt.StringType,
    sqltypes.VARCHAR: pyt.StringType,
}

# Iceberg v2 only supports us precision
os.environ["PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE"] = "1"


def access_token(
    token_endpoint: str, client_id: str, client_secret: str, scope: str
) -> str:
    response = requests.post(
        token_endpoint,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
    )
    response.raise_for_status()
    return response.json()["access_token"]


def make_db_url(conn_str: str) -> URL:
    """Create a DB URL

    If the driver is sqlite the database element is made into an absolute path.
    """
    db_url = make_url(conn_str)
    if db_url.drivername == "sqlite":
        db_url = db_url.set(database=str(Path(db_url.database).absolute()))  # type: ignore

    return db_url


def normalize_indentifier(identifier: str) -> str:
    """Performs transformation to snake_case.

    Simply takes captical letters as word separators and replaces with "_".
    """
    identifier = SNAKE_CASE_BREAK_1.sub(r"\1_\2", identifier)
    return SNAKE_CASE_BREAK_2.sub(r"\1_\2", identifier).lower()


def sql_to_iceberg_type(table_name: str, column: Column) -> pyt.IcebergType:
    try:
        return SQL_TO_ICEBERG_TYPES[type(column.type)]()
    except KeyError:
        raise TypeError(
            f"Column '{table_name}.{column.name}' has unsupported type '{column.type}'"
        )


def sql_to_iceberg_schema(
    engine: Engine, table_name, include_columns: Sequence[str] | None = None
):
    """Compute an Iceberg schema from the given table name.

    We use reflection from the original tables so that the 'required' flags within
    each schema field are set correctly, allowing us to use the upsert operation.
    The pyarrow schema from connectorx does not set the nullable field correctly
    but it is faster to read and create the pyarrow.Table.
    """
    meta = MetaData()
    sqltable = Table(table_name, meta)
    insp = inspect(engine)
    insp.reflect_table(sqltable, include_columns=include_columns)
    fields, identifier_field_ids = [], []
    for index, column in enumerate(sqltable.columns):
        id = index + 1
        fields.append(
            pyt.NestedField(
                id,
                normalize_indentifier(column.name),
                type=sql_to_iceberg_type(table_name, column),
                required=not column.nullable,
            )
        )
        if column.primary_key:
            identifier_field_ids.append(id)

    return Schema(*fields, identifier_field_ids=identifier_field_ids)


def sql_select(
    url: URL,
    table_identifier: str,
    columns: Sequence[str] | None = None,
    where: str | None = None,
) -> pa.Table:
    """Query the database and return the results with normalized identifiers"""
    if columns is None:
        columns = ["*"]
    query = f"select {','.join(columns)} from {table_identifier}"
    if where is not None:
        query += f" {where}"

    results: pa.Table = cx.read_sql(
        url.render_as_string(), query, protocol="binary", return_type="arrow"
    )
    return results.rename_columns(
        list(map(lambda x: normalize_indentifier(x), results.column_names))
    )


def extract_and_load(
    db_conn: URL,
    src_table: str,
    dest_table: Tuple[str, str],
    *,
    columns: Sequence[str] | None = None,
    where: str | None = None,
) -> int:
    src_query_results = sql_select(db_conn, src_table, columns, where)
    if catalog.table_exists(dest_table):
        tbl = catalog.load_table(dest_table)
    else:
        engine = create_engine(db_url)
        tbl = catalog.create_table(
            dest_table,
            sql_to_iceberg_schema(engine, src_table),
            properties={
                TableProperties.FORMAT_VERSION: TableProperties.DEFAULT_FORMAT_VERSION
            },
        )
    # This feels hacky but the pyarrow schema doesn't have the correct nullable fields so we force our
    # iceberg version on it.
    tbl.upsert(
        pa.Table.from_arrays(src_query_results.columns, schema=tbl.schema().as_arrow())
    )
    return src_query_results.num_rows


db_url = make_db_url(sys.argv[1])
print(f"Using source database {db_url}")

client_id, client_secret = "localinfra", "s3cr3t"
oauth2_server_uri = (
    "http://localhost:58080/auth/realms/iceberg/protocol/openid-connect/token"
)
token_scope = "lakekeeper"
catalog = RestCatalog(
    name="playground",
    warehouse="playground",
    uri="http://localhost:58080/iceberg/catalog/",
    credential=f"{client_id}:{client_secret}",
    **{
        "oauth2-server-uri": oauth2_server_uri,
        "scope": token_scope,
        "token": access_token(oauth2_server_uri, client_id, client_secret, token_scope),
        "downcast-ns-timestamp-to-us-on-write": True,
    },
)
catalog.create_namespace_if_not_exists(DATASET_NAME)


# extract
# examine LogbookEntryChanges for new changes
last_changeid = None
dest_logbook_entry_changes = (DATASET_NAME, LOGBOOK_ENTRY_CHANGES_LAST_SEEN)
if catalog.table_exists(dest_logbook_entry_changes):
    tbl = catalog.load_table(dest_logbook_entry_changes)
    df = tbl.scan().to_arrow()
    last_changeid = df["changeid"][0] if df.num_rows > 0 else None
print(f"Last ChangeId recorded in destination: {last_changeid}")

src_logbook_entry_changes = "LogbookEntryChanges"
src_query_results = sql_select(
    db_url,
    src_logbook_entry_changes,
    ["ChangeId", "EntryId"],
    where=f"WHERE ChangeId > {last_changeid}" if last_changeid is not None else None,
)
if src_query_results.num_rows == 0:
    print(
        f"Last ChangeId in {src_logbook_entry_changes} matches that in warehouse. No new records to process."
    )
    sys.exit(0)

min_entry_id = pc.min(src_query_results["entry_id"])
max_change_id = pc.max(src_query_results["change_id"])
print(
    f"Last ChangeId recored in source {max_change_id}. Affects entry_id > {min_entry_id}. Reloading entries."
)

# extract & load
tables = [
    {"name": "Entries", "where": f"where EntryId >= {min_entry_id}"},
    {"name": "MoreEntryColumns", "where": f"where EntryId >= {min_entry_id}"},
    {"name": "Logbooks"},
    {"name": "LogbookChapter"},
    {"name": "ChapterEntry"},
    {"name": "AdditionalColumns"},
]
for table_info in tables:
    src_table = table_info["name"]
    dest_table = (DATASET_NAME, normalize_indentifier(src_table))
    num_rows_loaded = extract_and_load(
        db_url,
        src_table,
        dest_table,
        where=table_info.get("where"),
    )
    print(f"Loaded {num_rows_loaded} rows into '{dest_table}'")


# save last change id loaded
max_change_id_tbl = pa.table({"changeid": [max_change_id]})
if catalog.table_exists(dest_logbook_entry_changes):
    tbl = catalog.load_table(dest_logbook_entry_changes)
else:
    tbl = catalog.create_table_if_not_exists(
        dest_logbook_entry_changes, max_change_id_tbl.schema
    )
tbl.overwrite(max_change_id_tbl)
