# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "openpyxl~=3.1.5",
#     "pandas~=2.3.1",
#     "pipelines-common",
# ]
#
# [tool.uv.sources]
# pipelines-common = { path = "../../../pipelines-common" }
# ///
import io
import pathlib
from typing import Iterator

import dlt
from dlt.common.storages.fsspec_filesystem import (
    FileItemDict,
)
from dlt.sources import TDataItems
import pendulum
import pandas as pd
from pipelines_common.cli import cli_main
from pipelines_common.dlt_sources.m365 import (
    sharepoint,
)

SITE_URL = "https://stfc365.sharepoint.com/sites/ISISSustainability"
PIPELINE_NAME = "electricity_sharepoint"
RDM_TIMEZONE = "Europe/London"


@dlt.transformer(section="m365")
def extract_content_and_read(
    items: Iterator[FileItemDict], skiprows: int
) -> Iterator[TDataItems]:
    """Extracts the file content and reads it assuming it is a .csv or a .xlsx file

    Combines the Date and Time columns into a DateTime field to simplify incremental
    processing.

    :param items: An iterator of dicts describing the file content
    :param skiprows: Number of rows in the csv/xlsx files to skip
    """

    # apply defaults to pandas kwargs
    kwargs = {"header": "infer", "skiprows": skiprows}

    for file_obj in items:
        file_content = io.BytesIO(file_obj.read_bytes())
        match pathlib.Path(file_obj["file_name"]).suffix:
            case ".csv":
                df = pd.read_csv(file_content, **kwargs)
            case ".xlsx":
                df = pd.read_excel(file_content, **kwargs)
            case _:
                raise RuntimeError(
                    f"Unsupported file extension in '{file_obj['file_name']}'"
                )

        # Store UTC not local timezone
        df["DateTime"] = (
            pd.to_datetime(df["Date"] + " " + df["Time"], format="%d/%m/%y %H:%M:%S")
            .dt.tz_localize(RDM_TIMEZONE)
            .dt.tz_convert("UTC")
        )
        df = df.drop(["Date", "Time"], axis=1)
        yield df.to_dict(orient="records")


@dlt.resource(merge_key="DateTime")
def rdm_data(
    datetime_cur=dlt.sources.incremental(
        "DateTime",
        initial_value=pendulum.DateTime.EPOCH,
    ),
) -> Iterator[TDataItems]:
    files = sharepoint(
        site_url=SITE_URL,
        file_glob="/General/RDM Data/**/*.*",
        extract_content=False,
        modified_after=datetime_cur.start_value,
    )
    reader = files | extract_content_and_read(
        **dlt.config[f"{PIPELINE_NAME}__pandas_read_kwargs"]
    )
    yield from reader


cli_main(
    pipeline_name=PIPELINE_NAME,
    default_destination="pipelines_common.dlt_destinations.pyiceberg",
    data_generator=rdm_data,
    dataset_name_suffix=PIPELINE_NAME,
    default_write_disposition="merge",
)
