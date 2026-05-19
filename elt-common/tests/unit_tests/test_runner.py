"""Tests for elt_common.runner"""

import json
from pathlib import Path
import re
from unittest.mock import MagicMock

import pyarrow as pa
from elt_common.iceberg.io import IcebergIO
import pytest
from pytest_mock import MockerFixture

from elt_common.runner import run_ingest
from elt_common.typing import ELTJobManifest

TEST_DOMAIN = "test_runner"


@pytest.fixture
def elt_job(request) -> ELTJobManifest:
    this_dir = Path(__file__).parent
    job_name = request.param

    return ELTJobManifest(
        name=job_name,
        domain=TEST_DOMAIN,
        ingest_job_dir=this_dir / "runner_test_fake_extract_defs" / job_name,
    )


@pytest.fixture
def mock_iceberg_io(mocker: MockerFixture):
    mocker.patch("elt_common.runner.connect_catalog", autospec=True)
    mock_iceberg_io_cls = mocker.patch("elt_common.runner.IcebergIO", autospec=True)
    mock_iceberg_io_cls.return_value = MagicMock(spec=IcebergIO)
    mock_iceberg_io = mock_iceberg_io_cls.return_value
    mock_iceberg_io.read_property.side_effect = KeyError

    return mock_iceberg_io


def _test_elt_job_table_id(elt_job: ELTJobManifest, table_name: str):
    return (f"{elt_job.domain}_{elt_job.name}", table_name)


@pytest.mark.parametrize("elt_job", ["working_impl"], indirect=True)
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
        call_args, call_kwargs = call.args, call.kwargs
        assert call_args[0] == _test_elt_job_table_id(elt_job, expected_table_names[index])
        data = call_args[1]
        assert isinstance(data, pa.Table)
        expected_table_name = expected_table_names[index]
        if expected_table_name != "empty":
            assert data["name"][0].as_py() == expected_table_name
        else:
            assert data.num_rows == 0
        assert call_args[2] == expected_write_modes[index]

        assert call_kwargs == {
            "merge_on": expected_merge_on[index],
            "partition": {},
            "sort_order": {},
            "watermark": None,
        }


@pytest.mark.parametrize("elt_job", ["watermark_handling"], indirect=True)
def test_run_ingest_with_watermark_handling(elt_job: ELTJobManifest, mock_iceberg_io: MagicMock):
    # Run first expecting full load
    rows_seen = run_ingest(elt_job)
    assert rows_seen == {"table_with_watermark_column": 20, "table_without_watermark_column": 10}

    # check IO write_table call
    call_args_list = mock_iceberg_io.write_table.call_args_list
    assert len(call_args_list) == 2
    (
        expected_table_names,
        expected_partition_by,
        expected_sortorder,
        expected_write_modes,
        expected_merge_on,
        expected_watermarks_after_extract,
    ) = (
        ["table_with_watermark_column", "table_without_watermark_column"],
        [{}, {}],
        [{}, {}],
        ("append", "append"),
        ([], []),
        [json.dumps({"column": "id", "value": 119}), None],
    )

    for index, call in enumerate(call_args_list):
        call_args, call_kwargs = call.args, call.kwargs
        assert call_args[0] == _test_elt_job_table_id(elt_job, expected_table_names[index])
        data = call_args[1]
        assert isinstance(data, pa.Table)
        expected_table_name = expected_table_names[index]
        if expected_table_name != "empty":
            assert data["name"][0].as_py() == expected_table_name
        else:
            assert data.num_rows == 0
        assert call_args[2] == expected_write_modes[index]

        # other arguments
        assert call_kwargs == {
            "merge_on": expected_merge_on[index],
            "partition": expected_partition_by[index],
            "sort_order": expected_sortorder[index],
            "watermark": expected_watermarks_after_extract[index],
        }

    # Run again expecting incremental load on table with watermark
    mock_iceberg_io.read_property.side_effect = [
        expected_watermarks_after_extract[0],
        KeyError,
    ]
    mock_iceberg_io.write_table.reset_mock()

    rows_seen = run_ingest(elt_job)
    assert rows_seen == {"table_with_watermark_column": 5, "table_without_watermark_column": 10}
    # check the table with watermark data has the expected values
    watermark_table_data = mock_iceberg_io.write_table.call_args_list[0].args[1]
    assert watermark_table_data["id"].to_pylist() == list(range(120, 125))


@pytest.mark.parametrize("elt_job", ["replace_multiple_yield"], indirect=True)
def test_run_ingest_with_write_mode_replace_first_replaces_then_appends(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock
):
    run_ingest(elt_job)

    call_args_list = mock_iceberg_io.write_table.call_args_list
    assert len(call_args_list) == 2

    # call 1 replaces
    first_call_args, first_call_kwargs = call_args_list[0].args, call_args_list[0].kwargs
    assert first_call_args[0] == _test_elt_job_table_id(elt_job, "table_replace_mode")
    data = first_call_args[1]
    assert isinstance(data, pa.Table)
    assert first_call_args[2] == "replace"
    assert first_call_kwargs == {
        "merge_on": [],
        "partition": {},
        "sort_order": {},
        "watermark": None,
    }

    # call 2 appends
    second_call_args = call_args_list[1].args
    assert second_call_args[0] == _test_elt_job_table_id(elt_job, "table_replace_mode")
    data = second_call_args[1]
    assert isinstance(data, pa.Table)
    assert second_call_args[2] == "append"


@pytest.mark.parametrize("elt_job", ["dynamic_tables"], indirect=True)
def test_run_ingest_with_dynamic_table_definitions(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock
):
    rows_seen = run_ingest(elt_job)

    assert mock_iceberg_io.write_table.call_count == 5
    for num in range(1, 6):
        table_name = f"table_{num}"
        assert table_name in rows_seen
        assert rows_seen[table_name] == 1


@pytest.mark.parametrize("elt_job", ["no_extract_cls"], indirect=True)
def test_run_ingest_without_extract_class_raises_error(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock
):
    with pytest.raises(AttributeError):
        run_ingest(elt_job)

    mock_iceberg_io.write_table.assert_not_called()


@pytest.mark.parametrize("elt_job", ["missing_definition_file"], indirect=True)
def test_run_ingest_with_missing_definition_file_raises_error(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock
):
    with pytest.raises(
        RuntimeError, match=re.compile(r"No extraction class definition file found.*")
    ):
        run_ingest(elt_job)

    mock_iceberg_io.write_table.assert_not_called()
