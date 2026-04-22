"""Tests for elt_common.runner"""

from pathlib import Path

from unittest.mock import _CallList, MagicMock

import pyarrow as pa
from elt_common.iceberg.io import IcebergIO
import pytest
from pytest_mock import MockerFixture

from elt_common.runner import run_ingest
from elt_common.typing import ELTJobManifest


@pytest.fixture
def elt_job() -> ELTJobManifest:
    this_dir = Path(__file__).parent

    return ELTJobManifest(
        name="fake_working_extract",
        domain="test_runner",
        ingest_job_dir=this_dir / "runner_test_fakes" / "fake_working_extract",
    )


def validate_write_table_calls(call_args_list: _CallList):
    assert len(call_args_list) == 2

    expected_table_names = ["table_default_write", "table_replace_mode"]
    expected_write_modes = ["append", "replace"]

    for index, call in enumerate(call_args_list):
        call_args, call_kwargs = call.args, call.kwargs
        assert call_args[0] == expected_table_names[index]
        data = call_args[1]
        assert isinstance(data, pa.Table)
        assert data["name"][0].as_py() == expected_table_names[index]
        for key, value in dict(
            merge_on=None,
            mode=expected_write_modes[index],
            partition=None,
            sort_order=None,
        ).items():
            assert call_kwargs[key] == value


class TestRunIngest:
    """Tests for run_ingest function"""

    def test_write_table(self, elt_job: ELTJobManifest, mocker: MockerFixture):
        mocker.patch("elt_common.runner.connect_catalog", autospec=True)
        mock_iceberg_io_cls = mocker.patch("elt_common.runner.IcebergIO", autospec=True)
        mock_iceberg_io_cls.return_value = MagicMock(spec=IcebergIO)
        run_ingest(elt_job)

        validate_write_table_calls(mock_iceberg_io_cls.return_value.write_table.call_args_list)
