# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "openpyxl>=3.1.5,<3.2",
# ]
# ///
import io
from typing import Any, Iterator

import dlt
from dlt.sources import TDataItems
from dlt.common.storages.fsspec_filesystem import FileItemDict
from pipelines_common.cli import cli_main
from pipelines_common.dlt_sources.m365 import sharepoint

SITE_URL = "https://stfc365.sharepoint.com/sites/ISIS-AcceleratorDivision"


@dlt.transformer(standalone=True)
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


# The FaultDate/FaultTime columns don't have consistent timestamp types in the excel data
# Read them as strings and convert in the transformation layer
def equipment_downtime_records_archive():
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


if __name__ == "__main__":
    cli_main(
        pipeline_name="accelerator_sharepoint",
        default_destination="pipelines_common.dlt_destinations.pyiceberg",
        data_generator=equipment_downtime_records_archive(),
        dataset_name_suffix="accelerator_sharepoint",
        default_write_disposition="replace",
    )
