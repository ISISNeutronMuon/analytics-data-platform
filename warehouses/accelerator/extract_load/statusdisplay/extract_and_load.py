#!/usr/bin/env -S uv run --script
# /// script
# requires-python = "==3.13.*"
# dependencies = [
#     "pandas",
#     "elt-common",
# ]
#
# [tool.uv.sources]
# elt-common = { path = "../../../../elt-common" }
# ///

import dlt
from dlt.extract import DltSource
from dlt.sources.rest_api import rest_api_source

import elt_common.cli as cli_utils


def statusdisplay() -> DltSource:
    return rest_api_source(
        name="api",
        config={
            "client": {
                "base_url": dlt.config["sources.base_url"],
            },
            "resources": dlt.config["sources.resources"],
            "resource_defaults": {
                "write_disposition": "replace",
            },
        },
    )


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="statusdisplay",
        default_destination="elt_common.dlt_destinations.pyiceberg",
        data_generator=statusdisplay(),
        dataset_name_suffix="statusdisplay",
    )
