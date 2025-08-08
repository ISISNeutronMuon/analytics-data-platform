# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas>=2.3.1,<2.4",
#     "pipelines-common",
# ]
#
# [tool.uv.sources]
# pipelines-common = { path = "../../pipelines-common" }
# ///
import io
from typing import Any, Iterator

import dlt
from dlt.common.storages.fsspec_filesystem import (
    FileItemDict,
)
from dlt.extract.resource import DltResource
from dlt.sources import TDataItems
import pendulum

from pipelines_common.cli import cli_main
from pipelines_common.dlt_sources.m365 import sharepoint

SITE_URL = "https://stfc365.sharepoint.com/sites/ISISSustainability"
PIPELINE_NAME = "electricity_sharepoint"
RDM_TIMEZONE = "Europe/London"

# The transformer is the last in the resource chain and is defined in dlt.souces.filesystem
# so the resource tries to lookup the config in sources.filesystem.credentials rather than
# sources.m365.credentials.
# read_csv.section = sharepoint.section


@dlt.transformer(section="m365")
def extract_content_and_read_csv(
    items: Iterator[FileItemDict], chunksize: int = 10000, **pandas_kwargs: Any
) -> Iterator[TDataItems]:
    """Extracts the file content and reads it assuming it is csv.

    Combines the Date and Time columns into a DateTime field to simplify incremental
    processing.
    Args:
        chunksize (int): Number of records to read in one chunk
        **pandas_kwargs: Additional keyword arguments passed to Pandas.read_csv
    Returns:
        TDataItem: The file content
    """
    import pandas as pd

    # apply defaults to pandas kwargs
    kwargs = {**{"header": "infer", "chunksize": chunksize}, **pandas_kwargs}

    for file_obj in items:
        for df in pd.read_csv(io.BytesIO(file_obj.read_bytes()), **kwargs):
            # Store UTC not local timezone
            df["DateTime"] = (
                pd.to_datetime(
                    df["Date"] + " " + df["Time"], format="%d/%m/%y %H:%M:%S"
                )
                .dt.tz_localize(RDM_TIMEZONE)
                .dt.tz_convert("UTC")
            )
            df = df.drop(["Date", "Time"], axis=1)
            yield df.to_dict(orient="records")


@dlt.resource(merge_key="datetime")
def rdm_data(
    datetime_cur=dlt.sources.incremental(
        "DateTime",
        initial_value=pendulum.from_format(
            "07/08/25 00:00:00", fmt="DD/MM/YY HH:mm:ss"
        ),
    ),
):
    files = sharepoint(
        site_url=SITE_URL,
        file_glob="/General/RDM Data/**/*.csv",
        extract_content=False,
        modified_after=datetime_cur.start_value,
    )
    reader = files | extract_content_and_read_csv(
        **dlt.config[f"{PIPELINE_NAME}__pandas_read_csv_kwargs"]
    )
    yield from reader

    # #.apply_hints(merge_key="datetime", incremental=datetime_col)
    # return reader.with_name("rdm_data")


cli_main(
    pipeline_name=PIPELINE_NAME,
    default_destination="pipelines_common.dlt_destinations.pyiceberg",
    data_generator=rdm_data,
    dataset_name_suffix=PIPELINE_NAME,
    default_write_disposition="merge",
)
