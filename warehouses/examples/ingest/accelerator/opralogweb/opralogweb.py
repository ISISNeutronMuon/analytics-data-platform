"""Pull data from the Opralogweb database"""

import functools as ft
from typing import Any, Sequence

from elt_common.typing import DataItems, TableItems
from elt_common.sources.sqldatabase import SqlDatabaseExtract
from sqlalchemy import Select


class Extract(SqlDatabaseExtract):
    def extract(self, table_info: TableItems) -> DataItems:
        # Pull out everything but Entries/MoreEntryColumns.
        append_only = {
            name: info
            for name, info in table_info.items()
            if name not in ("Entries", "MoreEntryColumns")
        }
        yield from super().extract(append_only)

        # Deal with updated record entries. The cursor column of the Entries table
        # is used to detect rows that have been modified in the source. These EntryIds of these
        # records are passed to the MoreEntyrColumns query to retrieve updates from that table
        # also.
        def entry_ids_in(table, query: Select[Any], entry_ids: Sequence[int]):
            # Modify the query to only select the given entry_ids
            return query.filter(table.c.EntryId.in_(entry_ids))

        with self._engine.connect() as conn:
            entries_info = table_info["Entries"]
            entries_chunk = next(self.extract_single(conn, "Entries", entries_info))
            yield entries_chunk
            loaded_entry_ids = entries_chunk[1].column("EntryId").to_pylist()

            yield from self.extract_single(
                conn,
                "MoreEntryColumns",
                table_info["MoreEntryColumns"],
                query_mutator=ft.partial(entry_ids_in, entry_ids=loaded_entry_ids),
            )
