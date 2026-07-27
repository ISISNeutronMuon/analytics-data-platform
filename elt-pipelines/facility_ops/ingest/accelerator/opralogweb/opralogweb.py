from itertools import batched

from html2text import html2text
import pyarrow as pa
import sqlalchemy as sa

from elt_common.extract import ResourceWriteProperties, ResourceProperties, Watermark
from elt_common.sources.sqldatabase import SqlDatabaseExtract, TableInfo

# Append only tables mapped to their id columns (for watermarking)
_append_tables = {
    "ChapterEntry": "LogbookEntryId",
    "LogbookChapter": "LogbookChapterNo",
    "Logbooks": "LogbookId",
    "AdditionalColumns": "AdditionalColumnId",
}


class Extract(SqlDatabaseExtract):
    def __init__(self, config):
        super().__init__(config)
        self._entry_ids = []

    def table_info(self):
        return {k: TableInfo(watermark_column=v) for k, v in _append_tables.items()}

    def extract_resource_properties(self):
        for name, props in super().extract_resource_properties():
            yield name, props

        # Tables which require custom extractor behaviour
        with self._engine.connect() as conn:
            # Extracts 'Entries' and stores their ids in self._entry_ids
            # Entries can be updated later, which will update their
            # 'LastChangedDate', so we use that for watermarking
            yield (
                "Entries",
                ResourceProperties(
                    extractor=lambda w: self._extract_entries(conn, w),
                    write_properties=ResourceWriteProperties(
                        write_mode="merge", merge_on=["EntryId"]
                    ),
                    watermark_column="LastChangedDate",
                ),
            )

            yield (
                "MoreEntryColumns",
                ResourceProperties(
                    extractor=lambda w: self._extract_more_entry_columns(conn, w),
                    write_properties=ResourceWriteProperties(
                        write_mode="merge", merge_on=["MoreEntryColumnId"]
                    ),
                    watermark_column=None,
                ),
            )

    def _extract_entries(self, conn: sa.Connection, w: Watermark | None):
        for entries in self._extract_table("Entries", conn=conn, watermark=w):
            chunk = _convert_comments_to_md(entries)

            # Track entry ids so we can fetch 'MoreEntryColumns' for them
            self._entry_ids.extend(chunk["EntryId"].to_pylist())

            yield chunk

    def _extract_more_entry_columns(self, conn: sa.Connection, _):
        # IN clauses can't be too big, so we batch them
        batched_ids = batched(self._entry_ids, 500)
        for batch in batched_ids:

            def entry_filter(query: sa.Select):
                return query.where(sa.column("EntryId").in_(batch))

            for chunk in self._extract_table(
                "MoreEntryColumns", conn=conn, query_filter=entry_filter
            ):
                yield chunk


def _convert_comments_to_md(entries: pa.Table):
    additional_comments_idx = entries.column_names.index("AdditionalComment")
    additional_comments = entries[additional_comments_idx].to_pylist()
    additional_comments = [
        html2text(c) if isinstance(c, str) else c for c in additional_comments
    ]
    a = pa.array(additional_comments, type=pa.string())
    return entries.set_column(additional_comments_idx, "AdditionalComment", a)
