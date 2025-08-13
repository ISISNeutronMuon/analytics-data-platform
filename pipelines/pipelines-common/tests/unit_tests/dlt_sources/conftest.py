import re
from tempfile import TemporaryDirectory
from typing import Dict

import dlt

import pytest
from pytest_httpx import HTTPXMock

from pipelines_common.dlt_sources.m365 import M365CredentialsResource


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

    @classmethod
    def mock_glob_responses(
        cls,
        httpx_mock: HTTPXMock,
        credentials: M365CredentialsResource,
        files_to_be_found: Dict[str, list],
        mock_fetch_content: bool,
    ):
        for dir_name, dir_contents in files_to_be_found.items():
            dir_path = f"/{dir_name}"
            # response for folder
            httpx_mock.add_response(
                method="GET",
                url=f"{credentials.api_url}/drives/{cls.library_id}/root:{dir_path}:",
                json={
                    "parentReference": {"path": "/drive/root:"},
                    "name": dir_name,
                    "folder": {"childCount": len(dir_contents)},
                },
                is_reusable=True,
            )
            # responses for file item info
            httpx_mock.add_response(
                method="GET",
                url=f"{credentials.api_url}/drives/{cls.library_id}/root:{dir_path}:/children",
                json={
                    "value": [
                        {
                            "id": f"{index}" * 10,
                            "name": file_item["name"],
                            "size": file_item["size"],
                            "file": {"mimeType": "image/png"},
                            "lastModifiedDateTime": file_item["mtime"],
                            "parentReference": {"path": f"/drive/root:/{dir_name}"},
                        }
                        for index, file_item in enumerate(dir_contents)
                    ]
                },
                is_reusable=True,
            )
            if mock_fetch_content:
                for file_item in dir_contents:
                    httpx_mock.add_response(
                        method="GET",
                        url=f"{credentials.api_url}/drives/{cls.library_id}/root:{dir_path}/{file_item['name']}:/content",
                        content=file_item["mtime"].encode("utf-8"),
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
