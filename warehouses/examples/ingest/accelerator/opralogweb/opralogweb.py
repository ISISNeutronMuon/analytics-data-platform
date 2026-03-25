"""Pull data from the Opralogweb database"""

import functools as ft
from typing import Any, Sequence

from elt_common.typing import (
    CursorInfo,
    DataChunk,
    DataItems,
    TableItems,
    TableProperties,
)
from elt_common.sources.sqldatabase import SqlDatabaseExtract
from html2text import html2text
import pyarrow as pa
from sqlalchemy import Select


class Extract(SqlDatabaseExtract):
    """Only mirror the fields we need for building the reports"""

    def tables(self) -> TableItems:
        return {
            "ChapterEntry": TableProperties(
                name="ChapterEntry",
                write_mode="append",
                cursor_column="LogbookEntryId",
                partition={"LogbookEntryId": "bucket[4]"},
                sort_order={"LogbookEntryId": "asc"},
            ),
            "LogbookChapter": TableProperties(
                name="LogbookChapter",
                write_mode="append",
                cursor_column="LogbookChapterNo",
                partition={"LogbookChapterNo": "bucket[4]"},
                sort_order={"LogbookChapterNo": "asc"},
            ),
            "Logbooks": TableProperties(
                name="Logbooks",
                write_mode="append",
                cursor_column="LogbookId",
                sort_order={"LogbookId": "asc"},
            ),
            "AdditionalColumns": TableProperties(
                name="AdditionalColumns",
                write_mode="append",
                cursor_column="AdditionalColumnId",
                sort_order={"AdditionalColumnId": "asc"},
            ),
            "Entries": TableProperties(
                name="Entries",
                write_mode="merge",
                merge_on=["EntryId"],
                cursor_column="LastChangedDate",
                partition={"EntryId": "bucket[8]"},
                sort_order={"EntryId": "asc"},
            ),
            "MoreEntryColumns": TableProperties(
                name="MoreEntryColumns",
                write_mode="merge",
                merge_on=["MoreEntryColumnId"],
                partition={"MoreEntryColumnId": "bucket[8]"},
                sort_order={"MoreEntryColumnId": "asc"},
            ),
        }

    def extract(self, cursor_info: CursorInfo) -> DataItems:
        # Pull out everything but Entries/MoreEntryColumns.
        table_info = self.tables()
        append_only = {
            name: info
            for name, info in table_info.items()
            if name not in ("Entries", "MoreEntryColumns")
        }
        with self._engine.connect() as conn:
            for name in append_only.keys():
                yield from super().extract_single(
                    conn, name, cursor_info=cursor_info.get(name)
                )

            # Deal with updated record entries. The cursor column of the Entries table is used
            # to detect rows that have been modified in the source. These EntryIds of these
            # records are passed to the MoreEntyrColumns query to retrieve updates from that table
            # also.
            try:
                entries_chunk = next(
                    self.extract_single(
                        conn, "Entries", cursor_info=cursor_info.get("Entries")
                    )
                )
                yield _to_markdown(entries_chunk, "AdditionalComment")
                loaded_entry_ids = entries_chunk[1].column("EntryId").to_pylist()

                yield from self.extract_single(
                    conn,
                    "MoreEntryColumns",
                    query_mutator=ft.partial(_entry_ids_in, entry_ids=loaded_entry_ids),
                )
            except StopIteration:
                # No new entries in source
                pass


def _entry_ids_in(table, query: Select[Any], entry_ids: Sequence[int | None]):
    # Modify the query to only select the given entry_ids
    if entry_ids:
        return query.filter(table.c.EntryId.in_(entry_ids))
    else:
        return query


def _to_markdown(data_item: DataChunk, column_name: str) -> DataChunk:
    """Transform the AdditionalComment column to markdown"""
    table_in = data_item[1]
    table_out = table_in.set_column(
        table_in.column_names.index(column_name),
        column_name,
        pa.array(
            table_in[column_name]
            .to_pandas()
            .apply(lambda x: x if x is None else html2text(x))
        ),
    )

    return (data_item[0], table_out)
