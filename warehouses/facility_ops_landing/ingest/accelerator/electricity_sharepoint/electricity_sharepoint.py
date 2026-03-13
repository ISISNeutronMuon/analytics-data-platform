# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas",
#     "elt-common[m365]",
#     "python-calamine",
# ]
#
# [tool.uv.sources]
# elt-common = { path = "../../../../../elt-common" }
# ///
import concurrent.futures
import io
import os
import pathlib
from typing import Iterator, Optional, Sequence

import dlt
import dlt.common.logger as logger
from dlt.sources import TDataItems
import pendulum
import pandas as pd
import pytz

from elt_common.cli import cli_main
from elt_common.dlt_sources.m365 import (
    sharepoint,
    M365DriveItem,
)
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    PartitionTrBuilder,
)

MAX_WORKERS_DEFAULT = min(8, (os.cpu_count() or 1) + 4)
EXCEL_ENGINE = "calamine"
ONE_DAY_SECS = 24 * 60 * 60
PIPELINE_NAME = "electricity_sharepoint"
RDM_TIMEZONE = "Europe/London"
SITE_URL = "https://stfc365.sharepoint.com/sites/ISISSustainability"

# File information
# The file can contain a record from a single export or multiple records combined. Each record starts with
# a header:

# Site Information:
# RAL ISIS RDM
# RAL ISIS RDM
# Controller: ISIS
# Controller description: ISIS Energy Totals
# Status: Online

# Time	Date	ISIS Elec Total Power {MW}
CSV_HEADER_ANCHOR = "time"


def effective_max_workers(max_workers: int | None) -> int:
    # For high-cpu-count machines the ThreadPoolExecutor default ends up being too high.
    # It has been observed that too many concurrent threads cause issues with partial file reads from
    # OneDrive. 8 threads seems to work correctly so clamp this as them maximum...

    return (
        min(MAX_WORKERS_DEFAULT, max_workers)
        if max_workers is not None
        else MAX_WORKERS_DEFAULT
    )


def to_utc(ts: pd.Series) -> pd.Series:
    """Assumes timezone unaware data and converts to UTC"""
    return ts.dt.tz_localize(RDM_TIMEZONE).dt.tz_convert("UTC")


def read_power_consumption_csv(file_content: io.BytesIO) -> pd.DataFrame:
    """Read csv-formatted power consumption records. This can return None if there is not data in the file.

    The Date & Time columns together to create a single DateTime column.
    """

    def csv_to_df(lines: Sequence[str]) -> pd.DataFrame:
        return pd.read_csv(io.StringIO("\n".join(lines)))

    metadata_anchor = "site information"
    sections, current_lines, in_data = [], [], False
    for line in file_content.getvalue().decode().splitlines():
        line = line.strip()
        line_lower = line.lower()

        if line_lower.startswith(CSV_HEADER_ANCHOR):
            # Save any previous section
            if current_lines:
                sections.append(csv_to_df(current_lines))
            # start new section with header
            current_lines = [line]
            in_data = True

        elif in_data:
            if line_lower.startswith(metadata_anchor):
                # Metadata block
                in_data = False
                continue
            else:
                current_lines.append(line)

    # the last section
    if current_lines:
        sections.append(csv_to_df(current_lines))

    # concatenate
    df = pd.concat(sections, ignore_index=True)

    # clean up column name (strip any whitespace)
    df.columns = df.columns.str.strip()
    df["DateTime"] = to_utc(
        pd.to_datetime(df["Date"] + " " + df["Time"], format="%d/%m/%y %H:%M:%S")  # type: ignore
    )
    return df.drop(["Date", "Time"], axis=1)


def read_power_consumption_excel(file_content: io.BytesIO) -> pd.DataFrame:
    """Read an excel-formatted power consumption record.

    Expected columns: Time, Total Power (MW)
    The Time column is renamed DateTime for consistency.
    """
    df = pd.read_excel(file_content, engine=EXCEL_ENGINE)
    # trim out header:
    #  - second column is all NaN until header
    #  - header itself starts with text defined in CSV_HEADER_ANCHOR
    # we want all rows where True
    df = df[
        df.iloc[:, 1].notna()
        & ~df.iloc[:, 0].str.contains(f"(?i){CSV_HEADER_ANCHOR}", na=False)
    ]
    df = df.set_axis(["DateTime", "ISIS Power MW"], axis=1)
    df["DateTime"] = to_utc(pd.to_datetime(df["DateTime"], format="ISO8601"))
    return df


@dlt.transformer(section="m365")
def extract_content_and_read(
    items: Iterator[M365DriveItem],
    max_workers: Optional[int] = dlt.config.value,
) -> Iterator[TDataItems]:
    """Extracts the file content and reads it assuming it is a .csv or a .xlsx file

    Combines the Date and Time columns into a DateTime field to simplify incremental
    processing.

    :param items: An iterator of dicts describing the file content
    :param max_workers (optional): How many threads to use to process the files.
                                   Defaults to a maximum defined by concurrent.futures.ThreadPoolExecutor
    """

    # The files are all independent. Process them in parallel and combine for a single yield
    def read_as_dataframe(file_obj: M365DriveItem) -> pd.DataFrame | None:
        file_name = file_obj["file_name"]
        file_bytes = file_obj.read_bytes()
        file_content = io.BytesIO(file_bytes)
        logger.debug(f"Filename '{file_name}' has size {len(file_bytes)} bytes.")
        try:
            match pathlib.Path(file_name).suffix:
                case ".csv":
                    df = read_power_consumption_csv(file_content)
                case ".xlsx":
                    df = read_power_consumption_excel(file_content)
                case _:
                    raise RuntimeError(f"Unsupported file extension in '{file_name}'")
        except pytz.exceptions.AmbiguousTimeError as exc:
            logger.warning(
                f"'Error loading {file_name} ({len(file_bytes)} bytes)'. There will be gaps in the data.\nDetails: {str(exc)}"
            )
            return None

        df["file_name"] = file_obj["file_name"]
        return df

    df_batch = None
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=effective_max_workers(max_workers)
    ) as executor:
        future_to_file_item = {
            executor.submit(read_as_dataframe, file_obj): file_obj for file_obj in items
        }
        for future in concurrent.futures.as_completed(future_to_file_item):
            df = future.result()
            if df is not None:
                df_batch = pd.concat((df_batch, df)) if df_batch is not None else df

    yield df_batch


@dlt.resource(merge_key="DateTime", columns={"DateTime": {"nullable": False}})
def rdm_data(
    datetime_cur=dlt.sources.incremental(
        "DateTime",
        initial_value=pendulum.DateTime.EPOCH,
    ),
) -> Iterator[TDataItems]:
    files = sharepoint(
        site_url=SITE_URL,
        extract_content=False,
        modified_after=datetime_cur.start_value
        - pendulum.Duration(seconds=ONE_DAY_SECS),
    )
    reader = files | extract_content_and_read()
    yield from reader.apply_hints(
        write_disposition="merge",
    )


if __name__ == "__main__":
    cli_main(
        pipeline_name=PIPELINE_NAME,
        data_generator=pyiceberg_adapter(
            rdm_data, partition=PartitionTrBuilder.year("date_time")
        ),
        source_domain="accelerator",
    )
