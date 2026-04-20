# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "elt-common",
#     "s3fs<2026.2.0"
# ]
#
# [tool.uv.sources]
# elt-common = { path = "../../../../../elt-common" }
# ///
"""Pull information about individual files on the archive"""

import glob
from pathlib import Path

import dlt
import elt_common.cli as cli_utils
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    PartitionTrBuilder,
)
import pyarrow as pa


@dlt.resource(
    merge_key=["file_name"],
    write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def archive_files(
    instrument_dirs_glob: str = dlt.config.value,
    data_dirs_glob: str = dlt.config.value,
):
    inst_data_dirs = [
        Path(inst_data_dir) for inst_data_dir in glob.glob(instrument_dirs_glob)
    ]
    if len(inst_data_dirs) == 0:
        raise RuntimeError(
            "Unable to find any instrument directories. Are the mounts present?"
        )

    for inst_data_dir in inst_data_dirs:
        for cycle_dir in inst_data_dir.glob(data_dirs_glob):
            file_records = [
                {
                    "instrument_dir": inst_data_dir.name,
                    "cycle_dir": cycle_dir.name,
                    "file_name": file.name,
                    "file_bytes": file.stat().st_size,
                }
                for file in cycle_dir.glob("*")
                if file.is_file()
            ]
            yield pa.Table.from_pylist(file_records)


if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="archive_files",
        source_domain="beamlines",
        data_generator=pyiceberg_adapter(
            archive_files,
            partition=[
                PartitionTrBuilder.identity("instrument_dir"),
                PartitionTrBuilder.identity("cycle_dir"),
            ],
        ),
    )
