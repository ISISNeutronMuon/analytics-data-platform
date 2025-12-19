#!/usr/bin/env -S uv run --script
# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "dlt[sql-database]",
#     "html2text==2025.4.15",
#     "pandas~=2.3.1",
#     "elt-common",
#     "pymssql~=2.3.4",
# ]
#
# [tool.uv.sources]
# elt-common = { path = "../../../../elt-common" }
# ///
from collections.abc import Generator
import datetime as dt

import dlt
from dlt.sources import DltResource
from dlt.sources.sql_database import sql_table
from html2text import html2text
import pyarrow as pa
from sqlalchemy.sql import Select

import elt_common.cli as cli_utils

OPRALOG_EPOCH = dt.datetime(2017, 4, 25, 0, 0, 0, tzinfo=dt.UTC)
SQL_TABLE_KWARGS = dict(
    schema=dlt.config.value, backend="pyarrow", backend_kwargs={"tz": "UTC"}
)

# Pass around extracted EntryIds
EXTRACTED_ENTRY_IDS: pa.Table = None


@dlt.source()
def opralogwebdb() -> Generator[DltResource]:
    """Opralog usage began in 04/2017. We split tables into two categories:

      - append-only tables: previous records are never updated, use 'append' write_disposition
      - merge-tables: old records could have been updated, use 'merge' write_disposition

    Both have incremental cursors to only load new or changed records. Unfortuantely
    the MoreEntryColumns table has no 'last changed' column to indicate when old records
    were updated. We use the 'LastChangedDate' column of 'Entries' to find the list of
    new or updated EntryId values and load these into the
    """

    tables_append_records = {
        "ChapterEntry": {"cursor": "LogbookEntryId"},
        "LogbookChapter": {"cursor": "LogbookChapterNo"},
        "Logbooks": {"cursor": "LogbookId"},
        "AdditionalColumns": {"cursor": "AdditionalColumnId"},
    }
    # Deal with simple, append-only tables first
    for name, info in tables_append_records.items():
        resource = sql_table(
            table=name,
            incremental=dlt.sources.incremental(info["cursor"]),
            write_disposition="append",
            **SQL_TABLE_KWARGS,
        )
        yield resource

    # Now the Entries table, with incremental cursor, that tells us what EntryIds have been updated
    yield entries_table()

    # Finally the MoreEntryColumns table based on the loaded EntryIds
    yield more_entry_columns_table()


def entries_table() -> DltResource:
    """Return a resource wrapper for the Entries table"""
    resource = sql_table(
        table="Entries",
        incremental=dlt.sources.incremental(
            "LastChangedDate",
            initial_value=OPRALOG_EPOCH,
            primary_key="EntryId",
        ),
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        **SQL_TABLE_KWARGS,
    )
    return resource.add_map(additional_comment_to_markdown).add_map(
        store_extracted_entry_ids
    )


def additional_comment_to_markdown(table: pa.Table) -> pa.Table:
    """Transform the AdditionalComment column to markdown"""
    column_name = "AdditionalComment"
    table = table.set_column(
        table.column_names.index(column_name),
        column_name,
        pa.array(
            table[column_name]
            .to_pandas()
            .apply(lambda x: x if x is None else html2text(x))
        ),
    )

    return table


def store_extracted_entry_ids(table: pa.Table) -> pa.Table:
    global EXTRACTED_ENTRY_IDS

    EXTRACTED_ENTRY_IDS = table["EntryId"]

    return table


def more_entry_columns_table() -> DltResource:
    """Return a resource wrapper for the MoreEntryColumns table"""

    def more_entry_columns_query(query: Select, table):
        return query.filter(table.c.EntryId.in_(EXTRACTED_ENTRY_IDS.to_pylist()))

    resource = sql_table(
        table="MoreEntryColumns",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        query_adapter_callback=more_entry_columns_query,
        **SQL_TABLE_KWARGS,
    )

    return resource


# ------------------------------------------------------------------------------

if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="opralogweb",
        default_destination="elt_common.dlt_destinations.pyiceberg",
        data_generator=opralogwebdb,
        dataset_name_suffix="opralogweb",
    )
