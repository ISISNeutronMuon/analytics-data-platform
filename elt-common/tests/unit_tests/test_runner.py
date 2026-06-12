"""Tests for elt_common.runner"""

import datetime as dt
import json
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pytest_mock import MockerFixture

from elt_common.iceberg.io import IcebergIO
from elt_common.runner import (
    INGEST_PROPERTY_KEY_LAST_UPDATED_AT,
    INGEST_PROPERTY_KEY_WATERMARK,
    run_ingest,
)
from elt_common.typing import ELTJobManifest

FAKE_NOW = dt.datetime(2026, 1, 1, 0, 0, 0)
TEST_DOMAIN = "test_runner"


@pytest.fixture
def patch_datetime_now(monkeypatch):
    class FixedNow(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return FAKE_NOW

    monkeypatch.setattr(dt, "datetime", FixedNow)


@pytest.fixture
def elt_job(request) -> ELTJobManifest:
    this_dir = Path(__file__).parent
    job_name = request.param

    return ELTJobManifest(
        name=job_name,
        domain=TEST_DOMAIN,
        ingest_job_dir=this_dir / "runner_extractor_fakes",
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
    return f"{elt_job.domain}_{elt_job.name}", table_name


def _assert_properties_as_expected(
    actual_properties: dict[str, str], expected_watermark_property: str | None = None
):
    last_updated_str = actual_properties[INGEST_PROPERTY_KEY_LAST_UPDATED_AT]
    assert dt.datetime.fromisoformat(last_updated_str) == FAKE_NOW

    if expected_watermark_property:
        assert actual_properties[INGEST_PROPERTY_KEY_WATERMARK] == expected_watermark_property


@pytest.mark.parametrize("elt_job", ["all_write_modes"], indirect=True)
def test_run_ingest_extract_using_all_write_modes(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock, patch_datetime_now
):
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

        assert call_kwargs["merge_on"] == expected_merge_on[index]
        assert call_kwargs["partition"] == {}
        assert call_kwargs["sort_order"] == {}
        _assert_properties_as_expected(call_kwargs["properties"])


@pytest.mark.parametrize("elt_job", ["watermark_handling"], indirect=True)
def test_run_ingest_watermark_handling(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock, patch_datetime_now
):
    # Run first expecting full load
    rows_seen = run_ingest(elt_job)
    assert rows_seen == {"table_with_watermark": 1000, "table_without_watermark": 500}

    # check IO write_table call
    call_args_list = mock_iceberg_io.write_table.call_args_list
    assert len(call_args_list) == 2

    (expected_table_names, expected_watermark_properties) = (
        ["table_with_watermark", "table_without_watermark"],
        [json.dumps({"column": "id", "value": 999}), None],
    )

    for index, call in enumerate(call_args_list):
        assert call.args[0] == _test_elt_job_table_id(elt_job, expected_table_names[index])
        data = call.args[1]
        assert isinstance(data, pa.Table)
        _assert_properties_as_expected(
            call.kwargs["properties"], expected_watermark_properties[index]
        )

    test_watermark_value = 600
    # Run again with a watermark set, and check that only the correct subset of rows are returned
    mock_iceberg_io.read_property.side_effect = [
        json.dumps({"column": "id", "value": test_watermark_value}),
        KeyError,
    ]
    mock_iceberg_io.write_table.reset_mock()

    rows_seen = run_ingest(elt_job)
    assert rows_seen == {
        "table_with_watermark": 1000 - test_watermark_value - 1,
        "table_without_watermark": 500,
    }
    call_args_list = mock_iceberg_io.write_table.call_args_list
    assert len(call_args_list) == 2
    watermarked_data = call_args_list[0].args[1]
    assert isinstance(watermarked_data, pa.Table)
    # Only rows with id greater than the watermark should be returned
    assert pc.min(watermarked_data["id"]).as_py() == test_watermark_value + 1


@pytest.mark.parametrize("elt_job", ["replace_multiple_yield"], indirect=True)
def test_run_ingest_with_write_mode_replace_first_replaces_then_appends(
    elt_job: ELTJobManifest, mock_iceberg_io: MagicMock
):
    run_ingest(elt_job)

    call_args_list = mock_iceberg_io.write_table.call_args_list
    assert len(call_args_list) == 2

    expected_table_name = _test_elt_job_table_id(elt_job, "table_replaced_with_multiple_chunks")
    # call 1 replaces
    first_call_args = call_args_list[0].args
    assert first_call_args[0] == expected_table_name
    data = first_call_args[1]
    assert isinstance(data, pa.Table)
    assert data.num_rows == 500
    assert first_call_args[2] == "replace"

    # call 2 appends
    second_call_args = call_args_list[1].args
    assert second_call_args[0] == expected_table_name
    data = second_call_args[1]
    assert isinstance(data, pa.Table)
    assert data.num_rows == 100
    assert second_call_args[2] == "append"
