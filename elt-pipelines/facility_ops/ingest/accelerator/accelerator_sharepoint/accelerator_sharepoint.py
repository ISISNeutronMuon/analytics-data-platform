"""Reads equipment downtime and mapping data from the accelerator SharePoint site"""

import io
from typing import Iterator

from elt_common.extract import BaseExtract, ResourceProperties, ResourceWriteProperties
from elt_common.sources.m365.client import SPListClient
from elt_common.sources.m365.credentials import M365Credentials

import pandas as pd
import pyarrow as pa

SITE_URL = "https://stfc365.sharepoint.com/sites/ISIS-AcceleratorDivision"


class Extract(BaseExtract):
    config_cls = M365Credentials

    def __init__(self, cfg: M365Credentials):
        super().__init__(cfg)
        self._client = SPListClient(SITE_URL, cfg)

    def extract_resource_properties(self) -> Iterator[tuple[str, ResourceProperties]]:
        yield (
            "equipment_downtime_data_11_08_24",
            ResourceProperties(
                extractor=self.extract_equipment_downtime,
                write_properties=ResourceWriteProperties(write_mode="replace"),
                watermark_column=None,
            ),
        )
        yield (
            "edr_equipment_mapping",
            ResourceProperties(
                extractor=self.extract_equipment_mapping,
                write_properties=ResourceWriteProperties(write_mode="replace"),
                watermark_column=None,
            ),
        )

    def _read_excel(self, path: str, **kwargs):
        file_content = self._client.read_file(path)
        with io.BytesIO(file_content) as f:
            df = pd.read_excel(f, **kwargs)
            return pa.Table.from_pandas(df)

    def extract_equipment_downtime(self, _):
        yield self._read_excel(
            "Beam Data/Equipment downtime data 11_08_24.xlsx",
            # Each of these columns change format partway down the spreadsheet,
            # so we read them as strings for now.
            #
            # A later transform step should clean these up to be a single datetime
            #
            # Note on the formats:
            # - At row 685, FaultDate changes from 'YYYY-MM-DD HH:mm:ss' to 'DD/MM/YYYY  HH:mm:ss' (note, two spaces)
            # - At row 10289, FaultTime changes from 'DD/MM/YYYY  HH:mm:ss' to 'HH:mm:ss'
            #   - Once this change happens, the time in FaultDate is always 00:00:00
            #   - Before the change, both fields have the same value (albeit with different formats for the first few hundred rows)
            dtype={"FaultDate": str, "FaultTime": str},
        )

    def extract_equipment_mapping(self, _):
        yield self._read_excel(
            "Beam Data/EDR Equipment Mapping.xlsx",
            header=None,
            names=["equipment_name", "equipment_category"],
        )
