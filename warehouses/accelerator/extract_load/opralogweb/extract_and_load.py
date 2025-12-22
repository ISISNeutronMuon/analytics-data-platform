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
from collections.abc import Generator, Iterator, Sequence

import dlt
from dlt.sources import DltResource
from dlt.sources.sql_database import sql_database
from html2text import html2text
import pyarrow as pa

import elt_common.cli as cli_utils


@dlt.transformer()
def html_to_markdown(
    table: pa.Table, *, column_names: Sequence[str]
) -> Iterator[pa.Table]:
    """Given a named set of columns, assuming they are HTML, transform the strings to markdown"""
    for name in column_names:
        if name not in table.column_names:
            continue

        table = table.set_column(
            table.column_names.index(name),
            name,
            pa.array(
                table[name]
                .to_pandas()
                .apply(lambda x: x if x is None else html2text(x))
            ),
        )

    yield table


def query_adapter_callback(query, table):
    if table.name in ("entries", "more_entry_columns"):
        try:
            import os

            oldest_changed_entry_id = int(os.environ["OLDEST_CHANGED_ENTRY_ID"])
            return query.where(table.c.entry_id >= oldest_changed_entry_id)
        except KeyError:
            pass

    return query


@dlt.source()
def opralogwebdb() -> Generator[DltResource]:
    """Pull the configured tables from the database backing the Opralog application"""
    tables = dlt.config["sources.sql_database.tables"]
    source = sql_database(
        query_adapter_callback=query_adapter_callback,
        schema=dlt.config.value,
        backend="pyarrow",
        backend_kwargs={"tz": "UTC"},
        table_names=[table_info["name"] for table_info in tables],
    )
    for table_info in tables:
        table_src_name = table_info["name"]
        resource = getattr(source, table_src_name)
        # Apply transformers first or resource hints are lost
        if "html_to_markdown_columns" in table_info:
            resource = resource | html_to_markdown(
                column_names=table_info["html_to_markdown_columns"]
            )
        if "write_disposition" in table_info:
            resource.apply_hints(
                write_disposition=table_info["write_disposition"],
            )
        if "incremental_id" in table_info:
            resource.apply_hints(
                incremental=dlt.sources.incremental(table_info["incremental_id"])
            )
        yield resource.with_name(table_info.get("destination_name", table_src_name))


# ------------------------------------------------------------------------------

if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="opralogweb",
        default_destination="elt_common.dlt_destinations.pyiceberg",
        data_generator=opralogwebdb,
        dataset_name_suffix="opralogweb",
    )
