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
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.sources.filesystem import read_csv
from pipelines_common.cli import cli_main
from pipelines_common.dlt_sources.m365 import sharepoint

SITE_URL = "https://stfc365.sharepoint.com/sites/ISISSustainability"
PIPELINE_NAME = "isis_electricity_sharepoint"

try:
    pandas_read_csv_kwargs = dlt.config[f"{PIPELINE_NAME}__pandas_read_csv_kwargs"]
except ConfigFieldMissingException:
    pandas_read_csv_kwargs = {}

files = sharepoint(
    site_url=SITE_URL,
    file_glob="/General/RDM Data/*.csv",
    extract_content=True,
)
reader = (files | read_csv(**pandas_read_csv_kwargs)).with_name("rdm_data")

cli_main(
    pipeline_name=PIPELINE_NAME,
    data_generator=reader,
    dataset_name_suffix=PIPELINE_NAME,
    default_write_disposition="replace",
)
