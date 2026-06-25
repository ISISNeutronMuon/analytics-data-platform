from datetime import datetime
import io
import json
import pyarrow.json
import requests

from elt_common.extract import BaseExtract, ResourceProperties, ResourceWriteProperties

CYCLES_URL = "https://status.isis.stfc.ac.uk/api/cycles"


class Extract(BaseExtract):
    def extract_resource_properties(self):
        yield (
            "elt_cycles",
            ResourceProperties(
                extractor=extract_cycles,
                write_properties=ResourceWriteProperties(write_mode="replace"),
                watermark_column=None,
            ),
        )


def extract_cycles(_):
    data = clean(fetch())
    newline_delimited = "\n".join(json.dumps(row) for row in data)

    with io.BytesIO(newline_delimited.encode()) as f:
        yield pyarrow.json.read_json(f)


def fetch():
    try:
        response = requests.get(CYCLES_URL, timeout=20)
    except requests.Timeout as ex:
        raise RuntimeError("Timed out when fetching cycles") from ex

    if not response.ok:
        raise RuntimeError(f"Failed to fetch cycles - {response.reason}")

    return response.json()


def reformat(date_string):
    """Convert a date from ISO format into one that pyarrow will convert into a timestamp"""
    return datetime.fromisoformat(date_string).strftime("%Y-%m-%d %H:%M:%S")


def clean(data):
    for cycle in data:
        for phase in cycle["phases"]:
            if "start" in phase:
                phase["start"] = reformat(phase["start"])
            if "end" in phase:
                phase["end"] = reformat(phase["end"])

    return data
