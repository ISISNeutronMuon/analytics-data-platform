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
    """Reads csv file with Pandas chunk by chunk.

    :param chunksize (int): Number of records to read in one chunk
    :param **pandas_kwargs: Additional keyword arguments passed to Pandas.read_csv
    :yield: TDataItem: The file content
    """
    import pandas as pd

    for file_obj in items:
        # Here we use pandas chunksize to read the file in chunks and avoid loading the whole file
        # in memory.
        df = pd.read_excel(io.BytesIO(file_obj["file_content"]))
        yield df.to_dict(orient="records")


files = sharepoint(
    site_url=SITE_URL,
    file_glob="Beam Data/Equipment downtime data 11_08_24.xlsx",
    extract_content=True,
)
reader = (files | read_excel()).with_name("equipment_downtime_data_historic")


cli_main(
    pipeline_name="accelerator_division_sharepoint",
    data_generator=reader,
    default_destination="duckdb",
    dataset_name_suffix="accelerator_division_sharepoint",
    default_write_disposition="replace",
)
