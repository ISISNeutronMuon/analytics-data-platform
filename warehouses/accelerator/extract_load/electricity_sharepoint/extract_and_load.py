# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas~=2.3.1",
#     "elt-common",
#     "python-calamine~=0.4.0",
# ]
#
# [tool.uv.sources]
# elt-common = { path = "../../../../elt-common" }
# ///
import concurrent.futures
import io
import pathlib
from typing import Iterator, Optional

import dlt
from dlt.sources import TDataItems
import pendulum
import pandas as pd
from elt_common.cli import cli_main
from elt_common.logging import logging
from elt_common.dlt_sources.m365 import (
    sharepoint,
    M365DriveItem,
)
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    pyiceberg_partition,
)

LOGGER = logging.getLogger(__name__)


EXCEL_ENGINE = "calamine"
PIPELINE_NAME = "electricity_sharepoint"
RDM_TIMEZONE = "Europe/London"
SITE_URL = "https://stfc365.sharepoint.com/sites/ISISSustainability"
LAG_WINDOW_SECONDS = 24 * 60 * 60


def to_utc(ts: pd.Series) -> pd.Series:
    """Assumes timezone unaware data and converts to UTC"""
    return ts.dt.tz_localize(RDM_TIMEZONE).dt.tz_convert("UTC")


def read_power_consumption_csv(
    file_content: io.BytesIO, skip_rows: int
) -> pd.DataFrame:
    """Read csv-formatted power consumption records. This can return None if there is not data in the file.

    Expected columns: Date, Time, Total Power (MW)
    The Date & Time columns together to create a single DateTime column.
    """
    df = pd.read_csv(file_content, skiprows=skip_rows)
    df["DateTime"] = to_utc(
        pd.to_datetime(df["Date"] + " " + df["Time"], format="%d/%m/%y %H:%M:%S")
    )
    return df.drop(["Date", "Time"], axis=1)


def read_power_consumption_excel(
    file_content: io.BytesIO, skip_rows: int
) -> pd.DataFrame:
    """Read an excel-formatted power consumption record.

    Expected columns: Time, Total Power (MW)
    The Time column is renamed DateTime for consistency.
    """
    df = pd.read_excel(file_content, engine=EXCEL_ENGINE, skiprows=skip_rows)
    df = df.rename(columns={"Time": "DateTime"})
    df["DateTime"] = to_utc(df["DateTime"])
    return df


@dlt.transformer(section="m365")
def extract_content_and_read(
    items: Iterator[M365DriveItem],
    skip_rows: int = dlt.config.value,
    max_threads: Optional[int] = dlt.config.value,
) -> Iterator[TDataItems]:
    """Extracts the file content and reads it assuming it is a .csv or a .xlsx file

    Combines the Date and Time columns into a DateTime field to simplify incremental
    processing.

    :param items: An iterator of dicts describing the file content
    :param skip_rows: Number of rows in the csv/xlsx files to skip
    :param max_threads (optional): How many threads to use to process the files. Defaults to concurrent.futures.ThreadPoolExecutor default value.
    """

    # The files are all independent. Process them in parallel and combine for a single yield
    def read_as_dataframe(file_obj: M365DriveItem) -> pd.DataFrame | None:
        file_content = io.BytesIO(file_obj.read_bytes())
        try:
            match pathlib.Path(file_obj["file_name"]).suffix:
                case ".csv":
                    df = read_power_consumption_csv(file_content, skip_rows)
                case ".xlsx":
                    df = read_power_consumption_excel(file_content, skip_rows)
                case _:
                    raise RuntimeError(
                        f"Unsupported file extension in '{file_obj['file_name']}'"
                    )
        except ValueError as exc:
            LOGGER.warning(
                f"Error reading '{file_obj['file_name']}': {str(exc)}. Skipping"
            )
            df = None

        return df

    df_batch = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_file_item = {
            executor.submit(read_as_dataframe, file_obj): file_obj for file_obj in items
        }
        for future in concurrent.futures.as_completed(future_to_file_item):
            df = future.result()
            if df is not None:
                df_batch = pd.concat((df_batch, df)) if df_batch is not None else df

    yield df_batch


# Occasionally pandas raises a EmptyDataError when parsing the CSV content indicating
# there is no data. This is likely an issue with a file being listed but no content yet available.
# We skip these files but want to make sure we grab them next time so we use the lag functionality
# to look back LAG_WINDOW_SECONDS when the next load is run.
@dlt.resource(merge_key="DateTime")
def rdm_data(
    datetime_cur=dlt.sources.incremental(
        "DateTime",
        initial_value=pendulum.DateTime.EPOCH,
        lag=LAG_WINDOW_SECONDS,
        last_value_func=max,
    ),
) -> Iterator[TDataItems]:
    files = sharepoint(
        site_url=SITE_URL,
        file_glob="/General/RDM Data/**/*.*",
        extract_content=False,
        modified_after=datetime_cur.start_value,
    )
    reader = files | extract_content_and_read()
    yield from reader


cli_main(
    pipeline_name=PIPELINE_NAME,
    default_destination="elt_common.dlt_destinations.pyiceberg",
    data_generator=pyiceberg_adapter(
        rdm_data, partition=pyiceberg_partition.year("date_time")
    ),
    dataset_name_suffix=PIPELINE_NAME,
    default_write_disposition="merge",
)
