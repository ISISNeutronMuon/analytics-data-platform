# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pandas>=2.3.1,<2.4",
#     "pipelines-common",
# ]
#
# [tool.uv.sources]
# pipelines-common = { path = "../../pipelines-common" }
# ///

import dlt
from dlt.extract.resource import DltResource
from dlt.sources.filesystem import read_csv
from pipelines_common.cli import cli_main
from pipelines_common.dlt_sources.m365 import sharepoint

SITE_URL = "https://stfc365.sharepoint.com/sites/ISISSustainability"
PIPELINE_NAME = "electricity_sharepoint"

# The transformer is the last in the resource chain and is defined in dlt.souces.filesystem
# so the resource tries to lookup the config in sources.filesystem.credentials rather than
# sources.m365.credentials.
read_csv.section = sharepoint.section


def rdm_data() -> DltResource:
    files = sharepoint(
        site_url=SITE_URL,
        file_glob="/General/RDM Data/*.csv",
        extract_content=True,
    )
    reader = files | read_csv(**dlt.config[f"{PIPELINE_NAME}__pandas_read_csv_kwargs"])
    return reader.with_name("rdm_data")


cli_main(
    pipeline_name=PIPELINE_NAME,
    default_destination="pipelines_common.dlt_destinations.pyiceberg",
    data_generator=rdm_data,
    dataset_name_suffix=PIPELINE_NAME,
    default_write_disposition="replace",
)
