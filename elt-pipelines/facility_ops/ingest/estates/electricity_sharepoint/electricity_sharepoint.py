"""Extracts measurements of ISIS's electricity usage from the ISIS Sustainability Sharepoint site"""

import concurrent.futures
import datetime as dt
import io
import os
import logging
import pathlib

import pandas as pd
import pyarrow as pa

from parsing import read_power_consumption_csv, read_power_consumption_excel

from elt_common.extract import (
    BaseExtract,
    ResourceProperties,
    Watermark,
    ResourceWriteProperties,
)
from elt_common.sources.m365.client import SPListClient, M365File
from elt_common.sources.m365.credentials import M365Credentials

SITE_URL = "https://stfc365.sharepoint.com/sites/ISISSustainability"
MAX_WORKERS = min(8, (os.cpu_count() or 1) + 4)

LOGGER = logging.getLogger(__name__)

_root_path = "/General/RDM Data"
_default_backfill_globs = [
    "**/*.xlsx",
    "**/*-daily.csv",
    "**/*-manual-export.csv",
]


class Configuration(M365Credentials):
    backfill: bool = False
    backfill_globs: list[str] = []

    @property
    def glob_patterns(self):
        if not self.backfill:
            return ["*-ISIS.csv"]
        return self.backfill_globs if self.backfill_globs else _default_backfill_globs


class Extract(BaseExtract):
    config_cls = Configuration

    def __init__(self, cfg: Configuration):
        super().__init__(cfg)
        self._client = SPListClient(SITE_URL, cfg)
        self._backfilling = cfg.backfill
        self._glob_patterns = cfg.glob_patterns

        LOGGER.debug(f"Searching for files matching: {self._glob_patterns}")

    def extract_resource_properties(self):
        yield (
            "rdm_data",
            ResourceProperties(
                extractor=self._extract_electricity_usage,
                write_properties=ResourceWriteProperties(
                    merge_on=["date_time"], write_mode="merge"
                ),
                watermark_column="date_time",
            ),
        )

    def _extract_electricity_usage(self, w: Watermark | None):
        watermark_value: dt.datetime | None = None
        if w and self._backfilling:
            LOGGER.debug("Ignoring watermark because this is a backfill")
        elif w:
            if not isinstance(w.value, dt.datetime):
                LOGGER.warning(
                    f"Ignoring watermark for electricity SP because it was '{w.value}', not a datetime"
                )
            else:
                watermark_value = w.value
                LOGGER.debug(f"Only fetching files modified after {watermark_value}")

        files = []
        for pattern in self._glob_patterns:
            files.extend(
                self._client.glob(
                    _root_path, pattern=pattern, modified_after=watermark_value
                )
            )

        LOGGER.debug(f"Matched {len(files)} files")

        resolved = self._read_files(files)
        df = read_contents_to_dataframe(resolved)
        if df is not None and df.size > 0:
            yield pa.Table.from_pandas(df, preserve_index=False)
        else:
            yield pa.Table.from_pylist([])

    def _read_files(self, files: list[M365File]) -> list[tuple[str, bytes]]:
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_file_item = {
                executor.submit(self._client.read_file, f.path): f for f in files
            }

            for future in concurrent.futures.as_completed(future_to_file_item):
                file = future_to_file_item[future]
                results.append((file.name, future.result()))
        return results


def read_contents_to_dataframe(files: list[tuple[str, bytes]]):
    """Extracts data from files into a single, combined dataframe

    :param files: (name, content) pairs for the files to extract data from
    """

    def read_as_dataframe(file_name, file_bytes) -> pd.DataFrame | None:
        file_content = io.BytesIO(file_bytes)
        LOGGER.debug(f"File '{file_name}' has size {len(file_bytes)} bytes.")
        match pathlib.Path(file_name).suffix:
            case ".csv":
                df = read_power_consumption_csv(file_name, file_content)
            case ".xlsx":
                df = read_power_consumption_excel(file_content)
            case _:
                raise RuntimeError(f"Unsupported file extension in '{file_name}'")

        if df is not None:
            df["file_name"] = file_name
        return df

    df_batch: pd.DataFrame | None = None
    for name, file_contents in files:
        df = read_as_dataframe(name, file_contents)
        if df is not None:
            df_batch = df if df_batch is None else pd.concat((df_batch, df))

    return df_batch
