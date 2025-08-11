import datetime
import itertools
from typing import cast, Iterator
import unittest.mock

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.pipeline.exceptions import PipelineStepFailed
import pendulum

from pipelines_common.dlt_sources.m365 import sharepoint, M365CredentialsResource

import pytest

from unit_tests.dlt_sources.conftest import SharePointTestSettings


def test_extract_sharepoint_source_raises_error_without_config(pipeline: dlt.Pipeline):
    # first without credentials
    with pytest.raises(PipelineStepFailed) as exc:
        pipeline.extract(sharepoint())

    config_exc = exc.value.exception
    assert isinstance(config_exc, ConfigFieldMissingException)
    for field in ("tenant_id", "client_id", "client_secret"):
        assert field in config_exc.fields

    # then other config
    with pytest.raises(PipelineStepFailed) as exc:
        pipeline.extract(sharepoint(credentials=M365CredentialsResource("tid", "cid", "cs3cr3t")))

    config_exc = exc.value.exception
    assert isinstance(config_exc, ConfigFieldMissingException)
    for field in ("site_url", "file_glob"):
        assert field in config_exc.fields


@pytest.mark.parametrize(
    ["files_per_page", "extract_content"],
    [
        (100, False),
        (100, True),
        (2, False),
    ],
)
def test_extract_sharepoint_yields_files_matching_glob(
    pipeline: dlt.Pipeline,
    mock_drivefs_cls: unittest.mock.MagicMock,
    files_per_page: int,
    extract_content: bool,
):
    num_files_found = 6
    drivefs_glob_return_value = {
        f"/Folder/{index}.csv": {
            "name": f"/Folder/{index}.csv",
            "type": "file",
            "mtime": datetime.datetime.fromisoformat("2025-01-01T00:00:00Z"),
            "size": "10",
        }
        for index in range(num_files_found)
    }
    if files_per_page >= num_files_found:
        expected_transfomer_calls = 1
        expected_transfomer_item_sizes = [num_files_found]
    else:
        expected_transfomer_calls = (
            int(num_files_found / files_per_page) + num_files_found % files_per_page
        )
        expected_transfomer_item_sizes = [
            sum(batch) for batch in itertools.batched([1] * num_files_found, files_per_page)
        ]

    transformer_calls = 0
    file_paths_seen = set()

    @dlt.transformer()
    def assert_expected_drive_items(drive_items: Iterator[FileItemDict]):
        nonlocal transformer_calls
        assert len(list(drive_items)) == expected_transfomer_item_sizes[transformer_calls]
        for drive_obj in drive_items:
            assert drive_obj["file_url"].startswith("m365:///Folder/")
            file_paths_seen.add(drive_obj["file_url"][len("m365://") :])
            assert drive_obj["modification_date"] == datetime.datetime.fromisoformat(
                "2025-01-01T00:00:00Z"
            )
            assert drive_obj["size_in_bytes"] == 10
            if extract_content:
                assert drive_obj["file_content"] == b"0123456789"

        transformer_calls += 1

    credentials = M365CredentialsResource("tid", "cid", "cs3cr3t")

    # Beware that the test_glob & resulting return values are not linked so changing test_glob
    # will not affect the glob.return_value. Here we are relying on the MSDDriveFS client to do the
    # right thing and we just test we call it correctly.
    test_glob = "/Folder/*.csv"
    mock_drivefs = mock_drivefs_cls.return_value
    mock_drivefs.glob.return_value = drivefs_glob_return_value
    mock_drivefs.open = unittest.mock.mock_open(read_data=b"0123456789")

    pipeline.extract(
        sharepoint(
            SharePointTestSettings.site_url,
            credentials=credentials,
            file_glob=test_glob,
            files_per_page=files_per_page,
            extract_content=extract_content,
        )
        | assert_expected_drive_items()
    )

    mock_drivefs_cls.assert_called_once_with(credentials, SharePointTestSettings.site_url)
    mock_drivefs.glob.assert_called_with(test_glob, detail=True)
    assert expected_transfomer_calls == transformer_calls
    # check we've seen each expected_file
    for file_path in drivefs_glob_return_value.keys():
        assert file_path in file_paths_seen


def test_extract_sharepoint_yields_files_matching_glob_respecting_modified_after(
    pipeline: dlt.Pipeline,
    mock_drivefs_cls: unittest.mock.MagicMock,
):
    num_files_matching_glob = 6
    drivefs_glob_return_value = {
        f"/Folder/{index}.csv": {
            "name": f"/Folder/{index}.csv",
            "type": "file",
            "mtime": datetime.datetime.fromisoformat("2025-01-01T00:00:00Z")
            + datetime.timedelta(minutes=1 * index),
            "size": "10",
        }
        for index in range(num_files_matching_glob)
    }

    modified_after = cast(pendulum.DateTime, pendulum.parse("2025-01-01T00:03:00Z"))
    num_files_matching_glob_and_modified_bound = 2
    transformer_called = False

    @dlt.transformer()
    def assert_expected_drive_items(drive_items: Iterator[FileItemDict]):
        nonlocal transformer_called
        transformer_called = True

        assert len(list(drive_items)) == num_files_matching_glob_and_modified_bound
        for drive_obj in drive_items:
            assert drive_obj["modification_date"] > modified_after

    credentials = M365CredentialsResource("tid", "cid", "cs3cr3t")
    # SharePointTestSettings.mock_get_drive_id_responses(httpx_mock, credentials)

    # Beware that the test_glob & resulting return values are not linked so changing test_glob
    # will not affect the glob.return_value. Here we are relying on the M365DriveFS client to do the
    # right thing and we just test we call it correctly.
    test_glob = "/Folder/*.csv"
    mock_drivefs = mock_drivefs_cls.return_value
    mock_drivefs.glob.return_value = drivefs_glob_return_value

    pipeline.extract(
        sharepoint(
            SharePointTestSettings.site_url,
            credentials=credentials,
            file_glob=test_glob,
            modified_after=modified_after,
        )
        | assert_expected_drive_items()
    )

    assert transformer_called
    mock_drivefs.glob.assert_called_with(test_glob, detail=True)
