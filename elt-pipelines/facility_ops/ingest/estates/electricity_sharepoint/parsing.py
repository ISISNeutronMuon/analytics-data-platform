"""Functions for parsing data from the electricity files

There are 3 known formats for the data. Each contains a preamble of the form:

Site Information:
RAL ISIS RDM
RAL ISIS RDM
Controller: ISIS
Controller description: ISIS Energy Totals
Status: Online

This preamble is discarded from all formats.

1. Excel (old, manual export format)
------------------------------------

These were produced by manual export from the original system.

There are two columns: "Time,Total Power (MW)". Time is in the format "YYYY-mm-DDTHH:MM:SS".

2. Automated CSV export
-----------------------

An automation deposits .csv files at regular intervals.

There are three columns: "Time,Date,ISIS Elec Total Power", where Time is in format HH:MM:SS and Date is in format DD/mm/YY.
Several of these files can be concatenated together to form larger timespan records - they are concatenated as is and repeated
preamble sections are not discarded. The variable CSV_PREAMBLE_ANCHOR defines the line at which a new section occurs.

3. Manual CSV export
--------------------

These were produced more recently by manual export from the original system.
There are three columns: "Time,ISIS Elec Total Energy,ISIS Elec Total Power", where Time is in format "DD/mm/YY HH:MM:SS".
The second column is discarded.
"""

import io
import logging
import pandas as pd

from typing import Sequence

CSV_PREAMBLE_ANCHOR = "time"
COL_DATE_TIME = "date_time"
COL_TOTAL_POWER = "isis_elec_total_power_mw"
EXCEL_SKIP_ROWS = 7
RDM_TIMEZONE = "Europe/London"

LOGGER = logging.getLogger(__name__)


def read_power_consumption_excel(file_content: io.BytesIO) -> pd.DataFrame:
    # See module comment for description of the format
    df_raw = pd.read_excel(file_content, skiprows=EXCEL_SKIP_ROWS)
    df_raw = df_raw.rename(
        columns={"Time": COL_DATE_TIME, "ISIS Elec Total Power {MW}": COL_TOTAL_POWER}
    )
    df_raw[COL_DATE_TIME] = _to_utc(df_raw[COL_DATE_TIME])  # type: ignore
    return df_raw


def read_power_consumption_csv(
    file_name: str, file_content: io.BytesIO
) -> pd.DataFrame | None:
    # See module comment for description of the format

    def _append_if_not_none(seq, df):
        if df is not None:
            seq.append(df)

    metadata_anchor = "site information"
    sections = []
    current_lines: list[str] = []
    in_data = False
    for line in file_content.getvalue().decode().splitlines():
        line = line.strip()
        line_lower = line.lower()

        if line_lower.startswith(CSV_PREAMBLE_ANCHOR):
            # Save any previous section
            if current_lines:
                _append_if_not_none(
                    sections, _csv_section_to_df(file_name, current_lines)
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
        _append_if_not_none(sections, _csv_section_to_df(file_name, current_lines))

    # concatenate
    if sections:
        return pd.concat(sections)

    return None


def _csv_section_to_df(file_name: str, lines: Sequence[str]) -> pd.DataFrame | None:
    """Parse csv and return a DataFrame if the times are valid. None if not"""
    buff = io.StringIO("\n".join(lines))
    df_raw: pd.DataFrame = pd.read_csv(buff)  # type: ignore

    # clean up column names (strip any whitespace)
    df_raw.columns = df_raw.columns.str.strip()
    cols = [c for c in df_raw.columns]
    if len(cols) != 3 or "power" not in cols[2].lower():
        LOGGER.warning(f"Columns in {file_name} are an unexpected format: {cols}")
        return None

    try:
        if cols[1].strip() == "Date":
            # Automated CSV format
            raw_datetime = df_raw["Date"] + " " + df_raw["Time"]
        else:
            # Manual CSV format
            raw_datetime = df_raw["Time"]

        df_datetime: pd.Series = pd.to_datetime(  # type: ignore
            raw_datetime, format="%d/%m/%y %H:%M:%S"
        )
        df = _to_utc(df_datetime).to_frame(name=COL_DATE_TIME)
    except ValueError as exc:
        # Pandas 3 uses ValueError for conversions that produce ambiguous/non-existent times
        msg = str(exc)
        if "ambiguous" in msg or "nonexistent" in msg:
            LOGGER.warning(
                f"'Error loading section of {file_name}'. DST issues detected: {str(exc)}"
            )
            return None
        else:
            raise

    df[COL_TOTAL_POWER] = df_raw[cols[2]]
    return df


def _to_utc(ts: pd.Series) -> pd.Series:
    """Assumes timezone unaware data and converts to UTC"""
    return ts.dt.tz_localize(RDM_TIMEZONE).dt.tz_convert("UTC")
