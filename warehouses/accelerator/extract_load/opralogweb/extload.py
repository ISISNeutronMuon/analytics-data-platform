from pathlib import Path
from typing import Sequence

import connectorx as cx
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType, LongType
import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy import create_engine, Engine, MetaData, Table
from sqlalchemy import inspect

import sys


DATASET_NAME = "src_opralogweb"
SQL_TO_ICEBERG_TYPES = {"INTEGER": LongType}
DB_PATH = Path(sys.argv[1])


def normalize_indentifier(identifier: str) -> str:
    return identifier.lower()


def db_to_iceberg_schema(
    engine: Engine, table_name, include_columns: Sequence[str] | None = None
):
    meta = MetaData()
    sqltable = Table(table_name, meta)
    insp = inspect(engine)
    insp.reflect_table(sqltable, include_columns=include_columns)
    fields, identifier_field_ids = [], []
    for index, column in enumerate(sqltable.columns):
        id = index + 1
        fields.append(
            NestedField(
                id,
                normalize_indentifier(column.name),
                type=SQL_TO_ICEBERG_TYPES[column.type.compile()](),
                required=not column.nullable,
            )
        )
        if column.primary_key:
            identifier_field_ids.append(id)

    return Schema(*fields, identifier_field_ids=identifier_field_ids)


print(f"Using source database {DB_PATH}")

client_id, client_secret = "localinfra", "s3cr3t"
catalog = RestCatalog(
    name="playground",
    warehouse="playground",
    uri="http://localhost:58080/iceberg/catalog/",
    credential=f"{client_id}:{client_secret}",
    **{
        "oauth2-server-uri": "http://localhost:58080/auth/realms/iceberg/protocol/openid-connect/token",
        "scope": "lakekeeper",
    },
)
catalog.create_namespace_if_not_exists(DATASET_NAME)


# extract
# examine LogbookEntryChanges for new changes
last_changeid = None
dest_logbook_entry_changes = (DATASET_NAME, "logbook_entry_changes_last")
if catalog.table_exists(dest_logbook_entry_changes):
    tbl = catalog.load_table(dest_logbook_entry_changes)
    df = tbl.scan().to_arrow()
    last_changeid = df["changeid"][0] if df.num_rows > 0 else None
print(f"Last ChangeId recorded in destination: {last_changeid}")

src_logbook_entry_changes = "LogbookEntryChanges"
where_clause = ""
if last_changeid is not None:
    where_clause = f"WHERE ChangeId > {last_changeid}"

query = f"SELECT ChangeId,EntryId FROM {src_logbook_entry_changes} {where_clause}"
src_query_results: pa.Table = cx.read_sql(
    f"sqlite://{DB_PATH}", query, protocol="binary", return_type="arrow"
)
src_query_results = src_query_results.rename_columns(
    list(map(lambda x: normalize_indentifier(x), src_query_results.column_names))
)
if src_query_results.num_rows == 0:
    print(
        f"Last ChangeId in {src_logbook_entry_changes} matches that in warehouse. No new records to process."
    )
    sys.exit(0)

min_entry_id = pc.min(src_query_results["entryid"])
max_change_id = pc.max(src_query_results["changeid"])
print(
    f"Last ChangeId recored in source {max_change_id}. Affects entry_id > {min_entry_id}. Reloading entries."
)

# load...

# save last change id loaded
max_change_id_tbl = pa.table({"changeid": [max_change_id]})
if catalog.table_exists(dest_logbook_entry_changes):
    tbl = catalog.load_table(dest_logbook_entry_changes)
else:
    engine = create_engine(f"sqlite:///{DB_PATH.absolute()}")
    tbl = catalog.create_table_if_not_exists(
        dest_logbook_entry_changes, max_change_id_tbl.schema
    )
tbl.overwrite(max_change_id_tbl)
