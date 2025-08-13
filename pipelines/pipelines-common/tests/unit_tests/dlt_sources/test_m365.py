import datetime
import itertools
from typing import cast, Iterator

import dlt
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.pipeline.exceptions import PipelineStepFailed
import pendulum

from pipelines_common.dlt_sources.m365 import sharepoint, M365CredentialsResource

import pytest
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
    files_available_count = 6
    files_available = {
        "Folder": [
            {
                "name": f"{index}.csv",
                "mtime": (
                    datetime.datetime(2025, 1, 1) + datetime.timedelta(minutes=1 * index)
                ).isoformat(),
                "size": "10",
            }
            for index in range(files_available_count)
        ]
    }

    if files_per_page >= files_available_count:
        expected_transfomer_calls = 1
        expected_transformer_item_sizes = [files_available_count if modified_after is None else 2]
    else:
        expected_transfomer_calls = (
            int(files_available_count / files_per_page) + files_available_count % files_per_page
        )
        expected_transformer_item_sizes = [
            sum(batch) for batch in itertools.batched([1] * files_available_count, files_per_page)
        ]

    transformer_calls = 0

    @dlt.transformer()
    def assert_expected_drive_items(drive_items: Iterator[FileItemDict]):
        nonlocal transformer_calls

        expected_items_size = expected_transformer_item_sizes[transformer_calls]
        drive_items_list = list(drive_items)
        assert len(drive_items_list) == expected_items_size
        for drive_obj in drive_items_list:
            assert drive_obj["file_url"].startswith("m365:///Folder/")
            assert drive_obj["size_in_bytes"] == 10
            assert isinstance((drive_obj["modification_date"]), datetime.datetime)
            if modified_after is not None:
                assert drive_obj["modification_date"] > modified_after
            if extract_content:
                assert (
                    pendulum.parse(drive_obj["file_content"].decode("utf-8"))
                    == drive_obj["modification_date"]
                )

        transformer_calls += 1

    credentials = M365CredentialsResource("tid", "cid", "cs3cr3t")
    SharePointTestSettings.mock_get_drive_id_responses(httpx_mock, credentials)
    test_glob = "/Folder/*.csv"  # if this is changed the mock responses will need updating
    SharePointTestSettings.mock_glob_responses(
        httpx_mock, credentials, files_available, extract_content
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
