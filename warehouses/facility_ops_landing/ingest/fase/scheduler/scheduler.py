# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "openpyxl",
#     "pandas",
#     "elt-common[m365]",
#     "s3fs<2026.2.0"

# ]
#
# [tool.uv.sources]
# elt-common = { path = "../../../../../elt-common" }
# ///
import io
from typing import Any, Iterator

import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from elt_common.cli import cli_main
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    PartitionTrBuilder,
)
from elt_common.dlt_sources.m365 import sharepoint
from dlt.sources import TDataItems


@dlt.transformer(section="m365")
def read_excel(
    items: Iterator[FileItemDict], **pandas_kwargs: Any
) -> Iterator[TDataItems]:
    """Reads an excel file with Pandas

    :param **pandas_kwargs: Additional keyword arguments passed to Pandas.read_excel
    :yield: TDataItem: The file content
    """
    import pandas as pd

    for file_obj in items:
        df = pd.read_excel(io.BytesIO(file_obj["file_content"]), **pandas_kwargs)
        yield df.to_dict(orient="records")


@dlt.resource()
def scheduled_experiments(
    site_url: str = dlt.config.value, file_glob: str = dlt.config.value
):
    """Return a resource to read the scheduled experiments export files.
    Note that this only contains anonymous data
    """
    files = sharepoint(
        site_url=site_url,
        file_glob=file_glob,
        extract_content=True,
    )
    reader = (
        (files | read_excel())
        .with_name("scheduled_experiments")
        .apply_hints(
            write_disposition="replace",
        )
    )
    yield reader


if __name__ == "__main__":
    cli_main(
        pipeline_name="scheduler",
        source_domain="fase",
        data_generator=pyiceberg_adapter(
            scheduled_experiments,
            partition=[
                PartitionTrBuilder.year("start_date"),
            ],
        ),
    )
