"""Tests for elt_common.runner"""

from pathlib import Path
import re
from unittest.mock import MagicMock

import pyarrow as pa
from elt_common.iceberg.io import IcebergIO
import pytest
from pytest_mock import MockerFixture

from elt_common.runner import run_ingest
from elt_common.typing import ELTJobManifest, ResourceProperties


@pytest.fixture
def elt_job(request) -> ELTJobManifest:
    this_dir = Path(__file__).parent
    job_name = request.param

    return ELTJobManifest(
        name=job_name,
        domain="test_runner",
        ingest_job_dir=this_dir / "runner_test_fake_extract_defs" / job_name,
    )


@pytest.fixture
def mock_iceberg_io(mocker: MockerFixture):
    mocker.patch("elt_common.runner.connect_catalog", autospec=True)
    mock_iceberg_io_cls = mocker.patch("elt_common.runner.IcebergIO", autospec=True)
    mock_iceberg_io_cls.return_value = MagicMock(spec=IcebergIO)

    return mock_iceberg_io_cls.return_value


@pytest.mark.parametrize("elt_job", ["simple"], indirect=True)
def test_run_ingest_with_simple_extract_class(elt_job: ELTJobManifest, mock_iceberg_io: MagicMock):
    run_ingest(elt_job)

    call_args_list = mock_iceberg_io.write_table.call_args_list
    assert len(call_args_list) == 4

    expected_table_names = [
        "table_default_write",
        "table_replace_mode",
        "table_merge_mode",
        "empty",
    ]
    expected_write_modes = ("append", "replace", "merge", "append")
    expected_merge_on = ([], [], ["name"], [])

    for index, call in enumerate(call_args_list):
        call_args = call.args
        assert call_args[0] == expected_table_names[index]
        assert call_args[1] == ResourceProperties(
            write_mode=expected_write_modes[index], merge_on=expected_merge_on[index]
        )
        data = call_args[2]
        assert isinstance(data, pa.Table)
        if expected_table_names[index] != "empty":
            assert data["name"][0].as_py() == expected_table_names[index]
        else:
            assert data.num_rows == 0


@pytest.mark.parametrize("elt_job", ["replace_multiple_yield"], indirect=True)
def test_run_ingest_with_write_mode_replace_first_replaces_then_appends(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock
):
    run_ingest(elt_job)

    call_args_list = mock_iceberg_io.write_table.call_args_list
    assert len(call_args_list) == 2

    # call 1 replaces
    first_call_args, first_call_kwargs = call_args_list[0].args, call_args_list[0].kwargs
    assert first_call_args[0] == "table_replace_mode"
    assert first_call_args[1] == ResourceProperties(write_mode="replace")
    data = first_call_args[2]
    assert isinstance(data, pa.Table)
    assert len(first_call_kwargs) == 0

    # call 2 appends
    second_call_args, second_call_kwargs = call_args_list[1].args, call_args_list[1].kwargs
    assert second_call_args[0] == "table_replace_mode"
    assert second_call_args[1] == ResourceProperties(write_mode="replace")
    data = second_call_args[2]
    assert isinstance(data, pa.Table)
    assert second_call_kwargs["force_write_mode"] == "append"


@pytest.mark.parametrize("elt_job", ["no_extract_cls"], indirect=True)
def test_run_ingest_without_extract_class_raises_error(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock
):
    with pytest.raises(AttributeError):
        run_ingest(elt_job)

    mock_iceberg_io.assert_not_called()


@pytest.mark.parametrize("elt_job", ["missing_definition_file"], indirect=True)
def test_run_ingest_with_missing_definition_file_raises_error(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock
):
    with pytest.raises(
        RuntimeError, match=re.compile(r"No extraction class definition file found.*")
    ):
        run_ingest(elt_job)

    mock_iceberg_io.assert_not_called()


@pytest.mark.parametrize("elt_job", ["missing_table_from_tables_method"], indirect=True)
def test_run_ingest_with_table_not_listed_in_tables_method_raises_error(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock
):
    with pytest.raises(
        ValueError,
        match=re.compile(
            r".*'table_not_listed' but its properties are not defined by the 'tables\(\)' method."
        ),
    ):
        run_ingest(elt_job)

    mock_iceberg_io.assert_not_called()
