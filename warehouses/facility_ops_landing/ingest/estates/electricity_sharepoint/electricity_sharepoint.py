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
from typing import Iterator, Sequence

import dlt
import dlt.common.logger as logger
from dlt.sources import TDataItems
import pendulum
import pandas as pd
import pyarrow.compute as pc
import pytz

from elt_common.cli import cli_main
from elt_common.dlt_sources.m365 import (
    sharepoint,
    M365DriveItem,
)
from elt_common.dlt_destinations.pyiceberg.helpers import load_iceberg_table
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    PartitionTrBuilder,
)

EXCEL_ENGINE = "calamine"
EXCEL_SKIP_ROWS = 7
MAX_WORKERS = min(8, (os.cpu_count() or 1) + 4)
ONE_DAY_SECS = 24 * 60 * 60
PIPELINE_NAME = "electricity_sharepoint"
RDM_TIMEZONE = "Europe/London"

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


def to_utc(ts: pd.Series) -> pd.Series:
    """Assumes timezone unaware data and converts to UTC"""
    return ts.dt.tz_localize(RDM_TIMEZONE).dt.tz_convert("UTC")


def csv_section_to_df(file_name: str, lines: Sequence[str]) -> pd.DataFrame | None:
    """Parse csv and return a DataFrame if the times are valid. None if not"""
    df = pd.read_csv(io.StringIO("\n".join(lines)))
    # clean up column name (strip any whitespace)
    df.columns = df.columns.str.strip()
    try:
        df["DateTime"] = to_utc(
            pd.to_datetime(df["Date"] + " " + df["Time"], format="%d/%m/%y %H:%M:%S")  # type: ignore
        )
        return df.drop(["Date", "Time"], axis=1)
    except pytz.exceptions.AmbiguousTimeError as exc:
        logger.warning(
            f"'Error loading section of {file_name}'. There will be gaps in the data.\nDetails: {str(exc)}"
        )
        return None


def read_power_consumption_csv(
    file_content: io.BytesIO,
    file_name: str,
) -> pd.DataFrame | None:
    """Read csv-formatted power consumption records. This can return None if there is not data in the file.

    The Date & Time columns together to create a single DateTime column.
    """

    def _append_if_not_none(seq, df):
        if df is not None:
            seq.append(df)

    metadata_anchor = "site information"
    sections, current_lines, in_data = [], [], False
    for line in file_content.getvalue().decode().splitlines():
        line = line.strip()
        line_lower = line.lower()

        if line_lower.startswith(CSV_HEADER_ANCHOR):
            # Save any previous section
            if current_lines:
                _append_if_not_none(
                    sections, csv_section_to_df(file_name, current_lines)
                )
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
        _append_if_not_none(sections, csv_section_to_df(file_name, current_lines))

    # concatenate
    if sections:
        return pd.concat(sections, ignore_index=True)

    return None


def read_power_consumption_excel(file_content: io.BytesIO) -> pd.DataFrame:
    """Read an excel-formatted power consumption record.

    Expected columns: Time, Total Power (MW)
    The Time column is renamed DateTime for consistency.
    """
    df = pd.read_excel(file_content, engine=EXCEL_ENGINE, skiprows=EXCEL_SKIP_ROWS)
    df = df.rename(columns={"Time": "DateTime"})
    df["DateTime"] = to_utc(df["DateTime"])
    return df


@dlt.transformer(section="m365")
def extract_content_and_read(items: Iterator[M365DriveItem]) -> Iterator[TDataItems]:
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
        match pathlib.Path(file_name).suffix:
            case ".csv":
                df = read_power_consumption_csv(file_content, file_name)
            case ".xlsx":
                df = read_power_consumption_excel(file_content)
            case _:
                raise RuntimeError(f"Unsupported file extension in '{file_name}'")

        return df

    df_batch = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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
    site_url: str = dlt.config.value,
    root_dir: str = dlt.config.value,
    backfill: bool = False,
) -> Iterator[TDataItems]:
    if backfill:
        historic_xl = sharepoint(
            site_url=site_url,
            file_glob=f"{root_dir}/**/*.xlsx",
            extract_content=False,
        )
        reader = historic_xl | extract_content_and_read()
        yield from reader.apply_hints(
            write_disposition="merge",
        )
        historic_csv = sharepoint(
            site_url=site_url,
            file_glob=f"{root_dir}/**/*-daily.csv",
            extract_content=False,
        )
        reader = historic_csv | extract_content_and_read()
        yield from reader.apply_hints(
            write_disposition="merge",
        )

    latest_files = sharepoint(
        site_url=site_url,
        file_glob=f"{root_dir}/*-ISIS.csv",
        extract_content=False,
        modified_after=get_latest_timestamp(dlt.current.pipeline()),
    )
    reader = latest_files | extract_content_and_read()
    yield from reader.apply_hints(
        write_disposition="merge",
    )


def get_latest_timestamp(pipeline: dlt.Pipeline) -> pendulum.DateTime | None:
    """Retrieve the timestamp loaded into the warehouse"""
    existing_rdm_data = load_iceberg_table(pipeline, "rdm_data")
    if existing_rdm_data is None:
        logger.debug("Peaks table does not exist. No runs have been loaded.")
        return None

    logger.debug("Peaks table exists, finding latest timestamp")
    c_datetime = "date_time"
    scan = existing_rdm_data.scan(selected_fields=(c_datetime,))
    running_max = None
    for batch in scan.to_arrow_batch_reader():
        batch_max = pc.max(batch[c_datetime])
        if batch_max.is_valid:
            running_max = (
                batch_max if running_max is None else pc.max([running_max, batch_max])
            )

    return pendulum.instance(running_max.as_py()) if running_max is not None else None


if __name__ == "__main__":
    cli_main(
        pipeline_name=PIPELINE_NAME,
        data_generator=pyiceberg_adapter(
            rdm_data, partition=PartitionTrBuilder.year("date_time")
        ),
        source_domain="estates",
    )
