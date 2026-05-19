import re

from elt_common.extract import ResourceProperties
import pyarrow as pa
import pytest


def dummy_extract(_):
    yield pa.table({"id": []})


def test_write_mode_limited_to_allowed_values():
    with pytest.raises(ValueError, match=re.compile(r"Invalid write mode.*")):
        ResourceProperties(extractor=dummy_extract, write_mode="invalid")  # type: ignore


def test_merge_write_mode_requires_merge_on_property():
    with pytest.raises(ValueError, match="'merge_on' must be provided when mode='merge'"):
        ResourceProperties(extractor=dummy_extract, write_mode="merge")
