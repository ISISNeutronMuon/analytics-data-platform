import re
from tempfile import TemporaryDirectory

import dlt

import pytest
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from pipelines_common.dlt_sources.m365 import M365CredentialsResource, M365DriveFS


class SharePointTestSettings:
    access_token = "857203984702drycbiluyweuicyrbweiucyrwuieyr2894729374FSDFSDFSFWC"
    hostname: str = "site.domain.com"
    site_path: str = "sites/MySite"
    site_url = f"https://{hostname}/{site_path}"
    site_id: str = (
        f"{hostname},0000000-1111-2222-3333-444444444444,55555555-6666-7777-8888-99999999999"
    )
    site_api_endpoint = f"sites/{hostname}:/{site_path}"
    library_id: str = "b!umnFmiec4Blh4I3Cv5be8uPs4IEW9cGoyL2iMLaafz2yjlrRtGbxqbR31mJiv5hCZfL"

    @classmethod
    def mock_get_drive_id_responses(
        cls, httpx_mock: HTTPXMock, credentials: M365CredentialsResource
    ):
        httpx_mock.add_response(
            method="POST",
            url=credentials.oauth2_token_endpoint,
            json={"access_token": SharePointTestSettings.access_token},
        )
        httpx_mock.add_response(
            method="GET",
            url=re.compile(f"{credentials.api_url}/{SharePointTestSettings.site_api_endpoint}?.*"),
            json={"id": SharePointTestSettings.site_id},
        )
        httpx_mock.add_response(
            method="GET",
            url=re.compile(
                f"{credentials.api_url}/sites/{SharePointTestSettings.site_id}/drive?.*"
            ),
            json={"id": SharePointTestSettings.library_id},
        )


@pytest.fixture
def pipeline():
    with TemporaryDirectory() as tmp_dir:
        pipeline = dlt.pipeline(
            pipeline_name="test_sharepoint_source_pipeline",
            pipelines_dir=tmp_dir,
            destination=dlt.destinations.duckdb(f"{tmp_dir}/data.db"),
            dataset_name="sharepoint_data",
            dev_mode=True,
        )
        yield pipeline


@pytest.fixture()
def mock_drivefs_cls(mocker: MockerFixture):
    patched_cls = mocker.patch("pipelines_common.dlt_sources.m365.M365DriveFS", autospec=True)
    patched_cls.protocol = M365DriveFS.protocol
    return patched_cls
