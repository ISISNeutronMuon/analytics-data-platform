# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "openpyxl>=3.1.5,<3.2",
#     "pandas>=2.2.3,<2.3.0",
#     "elt-common",
# ]
#
# [tool.uv.sources]
# elt-common = { path = "../../../../elt-common" }
# ///
import io
from typing import Any, Iterator

import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from elt_common.cli import cli_main
from elt_common.dlt_sources.m365 import sharepoint
from dlt.extract import DltResource
from dlt.sources import TDataItems

SITE_URL = "https://stfc365.sharepoint.com/sites/ISIS-AcceleratorDivision"


@dlt.transformer(section="m365")
def read_excel(
    items: Iterator[FileItemDict], **pandas_kwargs: Any
) -> Iterator[TDataItems]:
    """Reads an excel file with Pandas

    :param **pandas_kwargs: Additional keyword arguments passed to Pandas.read_excel
    :yield: TDataItem: The file content
    """
    import pandas as pd

    for file_obj in items:
        df = pd.read_excel(io.BytesIO(file_obj["file_content"]), **pandas_kwargs)
        yield df.to_dict(orient="records")


def equipment_downtime_records_archive() -> DltResource:
    """Return a resource to read the archived equipment downtime data.
    The FaultDate/FaultTime columns don't have consistent timestamp types in the excel data,
    read them as strings and convert in the transformation layer
    """
    files = sharepoint(
        site_url=SITE_URL,
        file_glob="Beam Data/Equipment downtime data 11_08_24.xlsx",
        extract_content=True,
    )
    reader = (
        (files | read_excel())
        .with_name("equipment_downtime_data_11_08_24")
        .apply_hints(
            columns={
                "FaultDate": {"data_type": "text"},
                "FaultTime": {"data_type": "text"},
            }
        )
    )
    return reader


def edr_equipment_mapping() -> DltResource:
    """Return a resource to read the excel file defining a mapping between old & new equipment names"""
    files = sharepoint(
        site_url=SITE_URL,
        file_glob="Beam Data/EDR Equipment Mapping.xlsx",
        extract_content=True,
    )
    return (
        files | read_excel(header=None, names=["equipment_name", "equipment_category"])
    ).with_name("edr_equipment_mapping")


# We need to set the section in order for dlt to find our named secret.
# We cannot run this in parallel as pyiceberg fails with
@dlt.source(section="m365")
def resources():
    for resource_func in (equipment_downtime_records_archive, edr_equipment_mapping):
        yield resource_func()


if __name__ == "__main__":
    cli_main(
        pipeline_name="accelerator_sharepoint",
        default_destination="elt_common.dlt_destinations.pyiceberg",
        data_generator=resources(),
        dataset_name_suffix="accelerator_sharepoint",
        default_write_disposition="replace",
    )
