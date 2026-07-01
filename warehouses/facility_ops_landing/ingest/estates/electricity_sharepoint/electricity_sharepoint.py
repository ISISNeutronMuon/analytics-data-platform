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
import pandas as pd
import pendulum
import pyarrow.compute as pc
import pytz
from dlt.sources import TDataItems
from elt_common.cli_utils import cli_main
from elt_common.dlt_destinations.pyiceberg.helpers import load_iceberg_table
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    PartitionTrBuilder,
    pyiceberg_adapter,
)
from elt_common.dlt_sources.m365 import (
    M365DriveItem,
    sharepoint,
)

EXCEL_ENGINE = "calamine"
EXCEL_SKIP_ROWS = 7
MAX_WORKERS = min(8, (os.cpu_count() or 1) + 4)
ONE_DAY_SECS = 24 * 60 * 60
PIPELINE_NAME = "electricity_sharepoint"
RDM_TIMEZONE = "Europe/London"

# Source details
#
# There are 3 known formats for the data. Each contains a preamble of the form:
#
# Site Information:
# RAL ISIS RDM
# RAL ISIS RDM
# Controller: ISIS
# Controller description: ISIS Energy Totals
# Status: Online
#
# This preamble is discarded from all formats.
#
# 1. Excel (old, manual export format)
# ------------------------------------
#
# These were produced by manual export from the original system.
#
# There are two columns: "Time,Total Power (MW)". Time is in the format "YYYY-mm-DDTHH:MM:SS".
#
# 2. Automated CSV export
# -----------------------
#
# An automation deposits .csv files at regular intervals.
#
# There are three columns: "Time,Date,ISIS Elec Total Power", where Time is in format HH:MM:SS and Date is in format DD/mm/YY.
# Several of these files can be concatenated together to form larger timespan records - they are concatenated as is and repeated
# preamble sections are not discarded.
#
# 3. Manual CSV export
# --------------------
#
# These were produced more recently by manual export from the original system.
# There are three columns: "Time,ISIS Elec Total Energy,ISIS Elec Total Power", where Time is in format "DD/mm/YY HH:MM:SS".
# The second column is discarded.


# Time	Date	ISIS Elec Total Power {MW}
CSV_PREAMBLE_ANCHOR = "time"


def to_utc(ts: pd.Series) -> pd.Series:
    """Assumes timezone unaware data and converts to UTC"""
    return ts.dt.tz_localize(RDM_TIMEZONE).dt.tz_convert("UTC")


def csv_section_to_df(file_name: str, lines: Sequence[str]) -> pd.DataFrame | None:
    """Parse csv and return a DataFrame if the times are valid. None if not"""
    df = pd.read_csv(io.StringIO("\n".join(lines)))
    # clean up column name (strip any whitespace)
    df.columns = df.columns.str.strip()
    cols = [c for c in df.columns]
    assert len(cols) == 3
    try:
        if "Date" in cols:
            # Automated CSV format
            df["DateTime"] = to_utc(
                pd.to_datetime(
                    df["Date"] + " " + df["Time"], format="%d/%m/%y %H:%M:%S"
                )  # type: ignore
            )
            df = df.drop(["Date", "Time"], axis=1)
        else:
            # Manual CSV format
            df["DateTime"] = to_utc(
                pd.to_datetime(df["Time"], format="%d/%m/%y %H:%M:%S")
            )
            # Drop time and energy columns
            df = df.drop(["Time", cols[1]], axis=1)
    except (pytz.exceptions.AmbiguousTimeError, ValueError) as exc:
        logger.warning(
            f"'Error loading section of {file_name}'. There will be gaps in the data.\nDetails: {str(exc)}"
        )
        return None

    return df


def read_power_consumption_csv(
    file_content: io.BytesIO,
    file_name: str,
) -> pd.DataFrame | None:
    # See comment at the top of this describing the format

    def _append_if_not_none(seq, df):
        if df is not None:
            seq.append(df)

    metadata_anchor = "site information"
    sections, current_lines, in_data = [], [], False
    for line in file_content.getvalue().decode().splitlines():
        line = line.strip()
        line_lower = line.lower()

        if line_lower.startswith(CSV_PREAMBLE_ANCHOR):
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
    # See comment at the top of this describing the format

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


@dlt.resource(
    primary_key="DateTime",
    write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def rdm_data(
    site_url: str = dlt.config.value,
    root_dir: str = dlt.config.value,
    backfill: bool = False,
    backfill_glob: str | None = None,
) -> Iterator[TDataItems]:
    if backfill:
        if backfill_glob is not None:
            file_globs = [backfill_glob]
        else:
            file_globs = [
                f"{root_dir}/**/*.xlsx",
                f"{root_dir}/**/*-daily.csv",
                f"{root_dir}/**/*-manual-export.csv",
            ]
        modified_after = None
    else:
        file_globs = [f"{root_dir}/*-ISIS.csv"]
        modified_after = get_latest_timestamp(dlt.current.pipeline())

    for file_glob in file_globs:
        file_listing = sharepoint(
            site_url=site_url,
            file_glob=file_glob,
            extract_content=False,
            modified_after=modified_after,
        )
        reader = file_listing | extract_content_and_read()
        yield from reader


def get_latest_timestamp(pipeline: dlt.Pipeline) -> pendulum.DateTime | None:
    """Retrieve the timestamp loaded into the warehouse"""
    existing_rdm_data = load_iceberg_table(pipeline, "rdm_data")
    if existing_rdm_data is None:
        return None

    logger.debug("Destination table exists, finding latest timestamp.")
    c_datetime = "date_time"
    scan = existing_rdm_data.scan(selected_fields=(c_datetime,))
    running_max = None
    for batch in scan.to_arrow_batch_reader():
        batch_max = pc.max(batch[c_datetime])  # type: ignore
        if batch_max.is_valid:
            running_max = (
                batch_max if running_max is None else pc.max([running_max, batch_max])  # type: ignore
            )

    latest_ts = (
        pendulum.instance(running_max.as_py()) if running_max is not None else None
    )
    logger.debug(f"Latest record has timestamp: {latest_ts}")
    return latest_ts


if __name__ == "__main__":
    cli_main(
        pipeline_name=PIPELINE_NAME,
        data_generator=pyiceberg_adapter(
            rdm_data, partition=PartitionTrBuilder.year("date_time")
        ),
        source_domain="estates",
    )
