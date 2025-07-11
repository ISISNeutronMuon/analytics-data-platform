#!/usr/bin/env -S uv run --script
# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "dlt[sql-database]>=1.10.0,<1.11.0",
#     "html2text==2025.4.15",
#     "pandas>=2.2.3,<2.3.0",
#     "pipelines-common",
#     "pymssql>=2.3.4,<2.4.0",
# ]
#
# [tool.uv.sources]
# pipelines-common = { path = "../../pipelines-common" }
# ///
from collections.abc import Generator, Iterator, Sequence

import dlt
from dlt.sources import DltResource
from dlt.sources.sql_database import sql_database
from html2text import html2text
import pyarrow as pa

import pipelines_common.cli as cli_utils


@dlt.transformer(standalone=True)
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


@dlt.source()
def opralogwebdb() -> Generator[DltResource]:
    """Pull the configured tables from the database backing the Opralog application"""
    tables = dlt.config["sources.sql_database.tables"]
    source = sql_database(
        schema=dlt.config.value,
        backend="pyarrow",
        backend_kwargs={"tz": "UTC"},
        table_names=[table_info["name"] for table_info in tables],
    )
    for table_info in tables:
        table_src_name = table_info["name"]
        resource = getattr(source, table_src_name)
        resource.apply_hints(
            incremental=dlt.sources.incremental(table_info["incremental_id"])
        )
        if "html_to_markdown_columns" in table_info:
            resource = resource | html_to_markdown(
                column_names=table_info["html_to_markdown_columns"]
            )
        yield resource.with_name(table_info.get("destination_name", table_src_name))


# ------------------------------------------------------------------------------

if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="opralogweb",
        default_destination="pipelines_common.dlt_destinations.pyiceberg",
        data_generator=opralogwebdb(),
        dataset_name_suffix="opralogweb",
        default_write_disposition="append",
    )
