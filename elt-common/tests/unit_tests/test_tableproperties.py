from elt_common.typing import TableProperties
import re
import pytest


def test_write_mode_limited_to_allowed_values():
    with pytest.raises(ValueError, match=re.compile(r"Invalid write_mode.*")):
        TableProperties(write_mode="invalid")  # type: ignore


def test_merge_write_mode_requires_merge_on_property():
    with pytest.raises(ValueError, match="'merge_on' must be provided when mode='merge'"):
        TableProperties(write_mode="merge")
