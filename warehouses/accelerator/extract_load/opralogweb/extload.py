import datetime as dt
from pathlib import Path
import re
import sys
from typing import Any, Literal, Mapping, Sequence, Tuple

import pandas as pd
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties
import pyiceberg.types as pyt
import pyarrow as pa
import pyarrow.compute as pc
import requests
import sqlalchemy.sql.sqltypes as sqltypes
from sqlalchemy import (
    create_engine,
    make_url,
    inspect,
    Column,
    Connection,
    Engine,
    MetaData,
    RowMapping,
    Table,
    URL,
)
from sqlalchemy.sql import text
import os


DATASET_NAME = "src_opralogweb"
LOGBOOK_ENTRY_CHANGES_LAST_SEEN = "logbook_entry_changes_last_seen"
SNAKE_CASE_BREAK_1 = re.compile("([^_])([A-Z][a-z]+)")
SNAKE_CASE_BREAK_2 = re.compile("([a-z0-9])([A-Z])")
SQL_TO_ICEBERG_TYPES = {
    sqltypes.Integer: pyt.IntegerType,
    sqltypes.INTEGER: pyt.IntegerType,
    sqltypes.BigInteger: pyt.LongType,
    sqltypes.BIGINT: pyt.LongType,
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
    src_type = column.type
    if isinstance(src_type, sqltypes.BigInteger):
        dest_type = pyt.LongType
    elif isinstance(src_type, sqltypes.Integer):
        dest_type = pyt.IntegerType
    elif isinstance(src_type, sqltypes.DateTime):
        dest_type = pyt.TimestamptzType
    elif isinstance(src_type, sqltypes.Double):
        dest_type = pyt.DoubleType
    elif isinstance(src_type, sqltypes.Float):
        dest_type = pyt.FloatType
    elif isinstance(src_type, (sqltypes.Text, sqltypes.VARCHAR, sqltypes.Uuid)):
        dest_type = pyt.StringType
    elif isinstance(src_type, sqltypes.Boolean):
        dest_type = pyt.BooleanType
    else:
        raise TypeError(
            f"Column '{table_name}.{column.name}' has unsupported type '{column.type}'"
        )

    return dest_type()


def sql_to_iceberg_schema(
    engine: Engine, table_name, include_columns: Sequence[str] | None = None
):
    """Compute an Iceberg schema from the given table name.

    We use reflection from the original tables so that the 'required' flags within
    each schema field are set correctly, allowing us to use the upsert operation.
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


def to_destination_schema(
    src_table: pa.Table, dest_schema: pa.Schema | None = None
) -> pa.Table:
    if dest_schema is None:
        return src_table

    src_schema = src_table.schema
    src_field_names, dest_field_names = src_schema.names, dest_schema.names
    assert len(src_field_names) == len(dest_field_names)
    for index, src_name in enumerate(src_field_names):
        if pa.types.is_string(src_schema.field(index).type) and pa.types.is_timestamp(
            dest_schema.field(index).type
        ):
            src_table[src_name] = pc.assume_timezone(
                pc.strptime(
                    src_table[src_name],
                    format="%Y-%m-%d %H:%M:%S.%f",
                    unit="us",  # or "ns" if you prefer
                ),
                "UTC",
            )

    return src_table


def str_to_timestamp(ts_str):
    return pa.scalar(
        dt.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f"),
        pa.timestamp("us", tz="UTC"),
    )


def rows_to_arrow_table(
    rows: Sequence[RowMapping], dest_schema: pa.Schema | None = None
) -> pa.Table:
    """Convert rows as return by sqlalchemy into an arrow Table.
    If the destination schema is provided then types are coerced
    """

    # tbl = pa.Table.from_pylist(rows)
    # if dest_schema is None:
    #     return tbl

    from pandas._libs import lib

    lib.to_object_array_tuples(rows).T

    # src_schema: pa.Schema = tbl.schema
    # for col_index, name in enumerate(src_schema.names):
    #     src_type = src_schema.field(col_index).type
    #     dest_type = dest_schema.field(col_index).type
    #     if pa.types.is_string(src_type) and pa.types.is_timestamp(dest_type):
    #         result = pc.call_function("str_to_timestamp", [tbl[name]])
    #         new_table = tbl.set_colum(col_index, name, str_to_timestamp(result))
    #     else:
    #         new_table = tbl

    # return new_table

    # for row in rows:
    #     for col, name in enumerate(dest_schema.names):
    #         dest_type = dest_schema.field(col).type
    #         value = row[col]
    #         if isinstance(value, str) and pa.types.is_timestamp(dest_type):
    #             row[col] = dt.datetime.strptime()

    # # Convert via Pandas to allow proper date/time conversion
    # df = pd.DataFrame.from_records(rows)  # type: ignore
    # df.columns = dest_schema.names

    # tbl = pa.Table.from_pandas(df, dest_schema)
    # return tbl


def sql_select_sqlalchemy(
    conn: Connection,
    table_identifier: str,
    columns: Sequence[str] | None = None,
    where: str | None = None,
    chunk_size: int | None = None,
    return_type: Literal["arrow", "rows"] = "arrow",
    arrow_schema: pa.Schema = None,
) -> pa.Table:
    """Query the database and return the results with normalized identifiers"""
    if columns is None:
        columns = ["*"]
    query = f"select {','.join(columns)} from {table_identifier}"
    if where is not None:
        query += f" {where}"

    cursor = conn.execute(text(query))
    if return_type == "arrow":
        yield rows_to_arrow_table(cursor.mappings().all(), arrow_schema)
    elif return_type == "rows":
        yield cursor.all()
    else:
        raise ValueError(
            f"Unknown return_type '{return_type}'. Allowed values: arrow, rows."
        )


def extract_and_load(
    db_conn: Connection,
    src_table: str,
    dest_table: Tuple[str, str],
    *,
    columns: Sequence[str] | None = None,
    where: str | None = None,
) -> int:
    iceberg_schema = sql_to_iceberg_schema(db_conn.engine, src_table)
    num_rows_total = 0
    src_query_pyarrow = next(
        sql_select_sqlalchemy(
            db_conn,
            src_table,
            columns,
            where,
            return_type="arrow",
            arrow_schema=iceberg_schema.as_arrow(),
        )
    )
    num_rows_total += src_query_pyarrow.num_rows
    print(f"Query yielded {src_query_pyarrow.num_rows} rows.")
    # if catalog.table_exists(dest_table):
    #     tbl = catalog.load_table(dest_table)
    # else:
    #     tbl = catalog.create_table(
    #         dest_table,
    #         iceberg_schema,
    #         properties={
    #             TableProperties.FORMAT_VERSION: TableProperties.DEFAULT_FORMAT_VERSION
    #         },
    #     )
    # tbl.upsert(src_query_pyarrow)
    return src_query_pyarrow.num_rows


db_url = make_db_url(sys.argv[1])
# db_url = (
#     "mssql+pymssql://dataplatform-ingest:Martyn!123@fitgensql1.isis.cclrc.ac.uk:1433"
# )
print(f"Using source database {db_url}")
db_engine = create_engine(db_url)


# client_id, client_secret = "localinfra", "s3cr3t"
# oauth2_server_uri = (
#     "http://localhost:58080/auth/realms/iceberg/protocol/openid-connect/token"
# )
# token_scope = "lakekeeper"
# catalog = RestCatalog(
#     name="playground",
#     warehouse="playground",
#     uri="http://localhost:58080/iceberg/catalog/",
#     credential=f"{client_id}:{client_secret}",
#     **{
#         "oauth2-server-uri": oauth2_server_uri,
#         "scope": token_scope,
#         "token": access_token(oauth2_server_uri, client_id, client_secret, token_scope),
#         "downcast-ns-timestamp-to-us-on-write": True,
#     },
# )
# catalog.create_namespace_if_not_exists(DATASET_NAME)

# examine LogbookEntryChanges for new changes
last_changeid = None
# dest_logbook_entry_changes = (DATASET_NAME, LOGBOOK_ENTRY_CHANGES_LAST_SEEN)
# if catalog.table_exists(dest_logbook_entry_changes):
#     tbl = catalog.load_table(dest_logbook_entry_changes)
#     df = tbl.scan().to_arrow()
#     last_changeid = df["changeid"][0] if df.num_rows > 0 else None
print(f"Last ChangeId recorded in destination: {last_changeid}")

src_logbook_entry_changes = "LogbookEntryChanges"
with db_engine.connect() as conn:
    rows = next(
        sql_select_sqlalchemy(
            conn,
            src_logbook_entry_changes,
            ["ChangeId", "EntryId"],
            where=f"WHERE ChangeId > {last_changeid} AND EntryId IS NOT NULL"
            if last_changeid is not None
            else None,
            chunk_size=None,
            return_type="rows",
        )
    )
if len(rows) == 0:
    print(
        f"Last ChangeId in {src_logbook_entry_changes} matches that in warehouse. No new records to process."
    )
    sys.exit(0)

min_entry_id = min(map(lambda x: x[1], rows))
max_change_id = max(map(lambda x: x[0], rows))
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
with db_engine.connect() as conn:
    for table_info in tables:
        src_table = table_info["name"]
        dest_table = (DATASET_NAME, normalize_indentifier(src_table))
        num_rows_loaded = extract_and_load(
            conn,
            src_table,
            dest_table,
            where=table_info.get("where"),
        )
        print(f"Loaded {num_rows_loaded} rows into '{dest_table}'")

# # save last change id loaded
# max_change_id_tbl = pa.table({"changeid": [max_change_id]})
# if catalog.table_exists(dest_logbook_entry_changes):
#     tbl = catalog.load_table(dest_logbook_entry_changes)
# else:
#     tbl = catalog.create_table_if_not_exists(
#         dest_logbook_entry_changes, max_change_id_tbl.schema
#     )
# tbl.overwrite(max_change_id_tbl)
