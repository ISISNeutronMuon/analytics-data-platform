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
"""Pull indexed metadata from beamlines"""

import glob
from pathlib import Path

import dlt
import elt_common.cli as cli_utils
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    PartitionTrBuilder,
)
import pyarrow as pa
from xml.etree import ElementTree as ET

NAPI_TO_PY = {
    "NX_CHAR": lambda x: x,
    "NX_INT32": lambda x: int(x),
    "NX_INT64": lambda x: int(x),
    "NX_UINT32": lambda x: int(x),
    "NX_UINT64": lambda x: int(x),
    "NX_FLOAT32": lambda x: float(x),
    "NX_FLOAT64": lambda x: float(x),
}
SCHEMA = "http://definition.nexusformat.org/schema/3.0"
SKIP_TAGS = ("IXtime_regime",)


def strip_ns(tag: str) -> str:
    """Remove any namespace information"""
    return tag.split("}", 1)[1] if "}" in tag else tag


def entry_to_dict(entry: ET.Element) -> dict:
    """Convert the XML entry to a record ready for a table"""
    record = {}
    for child in entry:
        tag = strip_ns(child.tag)
        text = child.text
        if tag in SKIP_TAGS or text is None:
            continue

        napi_type = child.attrib["NAPItype"]
        if "[" in napi_type:
            napi_type = napi_type[: napi_type.find("[")]
        try:
            record[tag] = NAPI_TO_PY[napi_type](text)
        except KeyError:
            record[tag] = text

    return record


def read_journal(journal: Path) -> pa.Table | None:
    try:
        tree = ET.parse(journal)
    except Exception:
        return None

    root = tree.getroot()
    entries = root.findall(f".//{{{SCHEMA}}}NXentry")
    records = [entry_to_dict(entry) for entry in entries]
    all_fields = sorted({k for rec in records for k in rec})
    columns = {field: [rec.get(field) for rec in records] for field in all_fields}

    return pa.Table.from_pydict(columns)


@dlt.resource(
    merge_key=["instrument_name", "run_number"],
    write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def journal_nxentries(
    instrument_dir_glob: str = dlt.config.value,
    journals_path_glob: str = dlt.config.value,
):
    instrument_dirs = list(glob.glob(instrument_dir_glob))
    if len(instrument_dirs) == 0:
        raise RuntimeError(
            "Unable to find any instrument directories. Are the mounts present."
        )

    for root_dir in instrument_dirs:
        for journal in Path(root_dir).glob(journals_path_glob):
            yield read_journal(journal)


if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="journals",
        source_domain="beamlines",
        data_generator=pyiceberg_adapter(
            journal_nxentries,
            partition=[
                PartitionTrBuilder.identity("instrument_name"),
            ],
        ),
    )
