# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas~=2.3.1",
#     "pipelines-common",
#     "python-calamine~=0.4.0",
# ]
#
# [tool.uv.sources]
# pipelines-common = { path = "../../../pipelines-common" }
# ///
import io
import pathlib
from typing import Iterator

import dlt
from dlt.sources import TDataItems
import pendulum
import pandas as pd
from pipelines_common.cli import cli_main
from pipelines_common.dlt_sources.m365 import (
    sharepoint,
    M365DriveItem,
)
from pipelines_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    pyiceberg_partition,
)


EXCEL_ENGINE = "calamine"
PIPELINE_NAME = "electricity_sharepoint"
RDM_TIMEZONE = "Europe/London"
SITE_URL = "https://stfc365.sharepoint.com/sites/ISISSustainability"


def to_utc(ts: pd.Series) -> pd.Series:
    """Assumes timezone unaware data and converts to UTC"""
    return ts.dt.tz_localize(RDM_TIMEZONE).dt.tz_convert("UTC")


def read_power_consumption_csv(file_content: io.BytesIO, skiprows: int) -> pd.DataFrame:
    """Read csv-formatted power consumption records.

    Expected columns: Date, Time, Total Power (MW)
    The Date & Time columns together to create a single DateTime column.
    """
    df = pd.read_csv(file_content, skiprows=skiprows)
    df["DateTime"] = to_utc(
        pd.to_datetime(df["Date"] + " " + df["Time"], format="%d/%m/%y %H:%M:%S")
    )
    return df.drop(["Date", "Time"], axis=1)


def read_power_consumption_excel(
    file_content: io.BytesIO, skiprows: int
) -> pd.DataFrame:
    """Read an excel-formatted power consumption record.

    Expected columns: Time, Total Power (MW)
    The Time column is renamed DateTime for consistency.
    """
    df = pd.read_excel(file_content, engine=EXCEL_ENGINE, skiprows=skiprows)
    df = df.rename(columns={"Time": "DateTime"})
    df["DateTime"] = to_utc(df["DateTime"])
    return df


@dlt.transformer(section="m365")
def extract_content_and_read(
    items: Iterator[M365DriveItem], skiprows: int
) -> Iterator[TDataItems]:
    """Extracts the file content and reads it assuming it is a .csv or a .xlsx file

    Combines the Date and Time columns into a DateTime field to simplify incremental
    processing.

    :param items: An iterator of dicts describing the file content
    :param skiprows: Number of rows in the csv/xlsx files to skip
    """
    # Each individual chunk is small so batch up the reads and yield a single DataFrame
    df_batch = None
    for file_obj in items:
        file_content = io.BytesIO(file_obj.read_bytes())
        match pathlib.Path(file_obj["file_name"]).suffix:
            case ".csv":
                df = read_power_consumption_csv(file_content, skiprows)
            case ".xlsx":
                df = read_power_consumption_excel(file_content, skiprows)
            case _:
                raise RuntimeError(
                    f"Unsupported file extension in '{file_obj['file_name']}'"
                )
        df_batch = pd.concat((df_batch, df)) if df_batch is not None else df

    yield df_batch


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
        **dlt.config[f"{PIPELINE_NAME}__pandas_read"]
    )
    yield from reader


cli_main(
    pipeline_name=PIPELINE_NAME,
    default_destination="pipelines_common.dlt_destinations.pyiceberg",
    data_generator=pyiceberg_adapter(
        rdm_data, partition=pyiceberg_partition.year("date_time")
    ),
    dataset_name_suffix=PIPELINE_NAME,
    default_write_disposition="merge",
)
