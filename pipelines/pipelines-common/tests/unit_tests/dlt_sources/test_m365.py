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
from pytest_mock import MockerFixture
from pytest_httpx import HTTPXMock
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
    ["files_per_page", "extract_content", "modified_after"],
    [
        (100, False, None),
        (100, True, None),
        (2, False, None),
        # The datetime provided here is based on the data set in conftest.py
        (100, False, cast(pendulum.DateTime, pendulum.parse("2025-01-01T00:03:00Z"))),
    ],
)
def test_extract_sharepoint_yields_files_matching_glob(
    pipeline: dlt.Pipeline,
    httpx_mock: HTTPXMock,
    files_per_page: int,
    extract_content: bool,
    modified_after: pendulum.DateTime | None,
):
    num_files_to_be_found = 6
    files_to_be_found = {
        "Folder": [
            {
                "name": f"{index}.csv",
                "mtime": (
                    datetime.datetime(2025, 1, 1) + datetime.timedelta(minutes=1 * index)
                ).isoformat(),
                "size": "10",
            }
            for index in range(num_files_to_be_found)
        ]
    }

    if files_per_page >= num_files_to_be_found:
        expected_transfomer_calls = 1
        expected_transformer_item_sizes = [num_files_to_be_found if modified_after is None else 2]
    else:
        expected_transfomer_calls = (
            int(num_files_to_be_found / files_per_page) + num_files_to_be_found % files_per_page
        )
        expected_transformer_item_sizes = [
            sum(batch) for batch in itertools.batched([1] * num_files_to_be_found, files_per_page)
        ]

    transformer_calls = 0
    file_paths_seen = set()

    @dlt.transformer()
    def assert_expected_drive_items(drive_items: Iterator[FileItemDict]):
        nonlocal transformer_calls
        expected_items_size = expected_transformer_item_sizes[transformer_calls]
        drive_items_list = list(drive_items)
        assert len(drive_items_list) == expected_items_size
        for drive_obj in drive_items_list[-expected_items_size:]:
            assert drive_obj["file_url"].startswith("m365:///Folder/")
            file_paths_seen.add(drive_obj["file_url"][len("m365://") :])
            assert isinstance((drive_obj["modification_date"]), datetime.datetime)
            assert drive_obj["size_in_bytes"] == 10
            if extract_content:
                assert drive_obj["file_content"] == b"0123456789"

        transformer_calls += 1

    credentials = M365CredentialsResource("tid", "cid", "cs3cr3t")
    SharePointTestSettings.mock_get_drive_id_responses(httpx_mock, credentials)

    test_glob = "/Folder/*.csv"  # if this is changed the mock responses will need updating
    SharePointTestSettings.mock_glob_responses(
        httpx_mock, credentials, files_to_be_found, extract_content
    )
    pipeline.extract(
        sharepoint(
            SharePointTestSettings.site_url,
            credentials=credentials,
            file_glob=test_glob,
            files_per_page=files_per_page,
            extract_content=extract_content,
            modified_after=modified_after,
        )
        | assert_expected_drive_items()
    )

    assert expected_transfomer_calls == transformer_calls
    # check we've seen each expected files
    for dir_name, dir_contents in files_to_be_found.items():
        for file_item in dir_contents[:]:
            assert f"/{dir_name}/{file_item['name']}" in file_paths_seen
