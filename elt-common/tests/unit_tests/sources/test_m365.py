import datetime as dt
import re

import pytest
from pydantic import SecretStr
from pytest_httpx import HTTPXMock

from elt_common.sources.m365.client import M365File, SPListClient, _api_url
from elt_common.sources.m365.configuration import M365Config, M365Credentials

hostname = "site.domain.com"
site_path = "sites/MySite"
site_url = f"https://{hostname}/{site_path}"
site_id = f"{hostname},0000000-1111-2222-3333-444444444444,55555555-6666-7777-8888-99999999999"
site_api_endpoint = f"sites/{hostname}:/{site_path}"
library_id = "b!umnFmiec4Blh4I3Cv5be8uPs4IEW9cGoyL2iMLaafz2yjlrRtGbxqbR31mJiv5hCZfL"

_credentials = M365Credentials(
    tenant_id="tenant", client_id="client", client_secret=SecretStr("secret")
)


@pytest.fixture
def list_client(httpx_mock: HTTPXMock):
    """Instantiates a client for testing, mocking the network calls it uses for setup"""

    # Mock the call which retrieves an access token
    httpx_mock.add_response(
        method="POST",
        url=_credentials._oauth2_token_endpoint,
        json={"access_token": "fake_token"},
    )

    # Mock the calls retrieving the drive id for the site
    httpx_mock.add_response(
        method="GET",
        url=re.compile(f"{_api_url}/{site_api_endpoint}?.*"),
        json={"id": site_id},
    )
    httpx_mock.add_response(
        method="GET",
        url=re.compile(f"{_api_url}/sites/{site_id}/drive?.*"),
        json={"id": library_id},
    )

    return SPListClient(
        M365Config(
            site_url=site_url,
            credentials=_credentials,
        )
    )


@pytest.fixture
def fake_files():
    return {
        "time-periods": [
            {"name": "2500s", "lastModifiedDateTime": "2500-01-01T00:00:00.00Z"},
            {"name": "2100s", "lastModifiedDateTime": "2150-01-01T00:00:00.00Z"},
            {
                "name": "now",
                "lastModifiedDateTime": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            },
            {"name": "2000s", "lastModifiedDateTime": "2010-01-01T00:00:00.00Z"},
            {"name": "1970s", "lastModifiedDateTime": "1971-01-01T00:00:00.00Z"},
        ],
        "many-formats": [
            {"name": f"a.{ext}", "lastModifiedDateTime": "2000-01-01T00:00:00.00Z"}
            for ext in ["txt", "csv", "xlsx", "xls", "abc123"]
        ],
    }


def test_glob_filtering(list_client, fake_files):
    """Test that filter options work on fake data"""

    def mocked_read_tree(folder):
        return [M365File.from_sp_file(f, folder) for f in fake_files[folder]]

    list_client._read_tree = mocked_read_tree

    def glob_many_formats(*args, **kwargs):
        return list_client.glob("many-formats", *args, **kwargs)

    def glob_time_periods(*args, **kwargs):
        return list_client.glob("time-periods", *args, **kwargs)

    ten_secs_ago = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=-10)
    in_ten_secs = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=10)

    # Tests for modified_after filter
    assert len(glob_time_periods()) == len(fake_files["time-periods"])
    assert len(glob_time_periods(modified_after=ten_secs_ago)) == 3
    assert len(glob_time_periods(modified_after=in_ten_secs)) == 2

    # Tests for path filter
    assert len(glob_many_formats()) == len(fake_files["many-formats"])
    assert len(glob_many_formats("*")) == len(fake_files["many-formats"])
    assert len(glob_many_formats("many-formats/*")) == len(fake_files["many-formats"])
    assert len(glob_many_formats("**/*")) == len(fake_files["many-formats"])
    assert len(glob_many_formats("*.csv")) == 1
    assert len(glob_many_formats("*.xls")) == 1
    assert len(glob_many_formats("*.xls*")) == 2
    assert len(glob_many_formats("**/*.xls*")) == 2
    assert len(glob_many_formats("**/*/*.xls*")) == 0

    # Tests for both combined
    assert len(glob_time_periods(pattern="time-periods/now", modified_after=ten_secs_ago)) == 1
    assert len(glob_time_periods(pattern="**/now", modified_after=ten_secs_ago)) == 1
    assert len(glob_time_periods(pattern="**/2*", modified_after=ten_secs_ago)) == 2
    assert len(glob_time_periods(pattern="**/*", modified_after=ten_secs_ago)) == 3
    assert len(glob_time_periods(pattern="**/nout", modified_after=ten_secs_ago)) == 0


def test_multiple_pages_of_items(list_client, httpx_mock):
    """Test that the client follows links for extra pages"""
    num_next_pages = 5
    num_sub_page_items = 10
    httpx_mock.add_response(
        method="GET",
        url=re.compile(f"{_api_url}/drives/{library_id}/root:/test_folder:/children?.+"),
        json={
            "value": [{"name": "root_item", "lastModifiedDateTime": "2026-07-13T00:00:00.00Z"}],
            "@odata.nextLink": "https://www.page0.com",
        },
    )

    for i in range(num_next_pages):
        httpx_mock.add_response(
            method="GET",
            url=f"https://www.page{i}.com",
            json={
                "value": [
                    {
                        "name": f"page_{i}_item_{j}",
                        "lastModifiedDateTime": "2026-07-13T00:00:00.00Z",
                    }
                    for j in range(num_sub_page_items)
                ],
                "@odata.nextLink": None
                if i == num_next_pages - 1
                else f"https://www.page{i + 1}.com",
            },
        )
    result = list_client.glob("test_folder")
    assert len(result) == num_next_pages * num_sub_page_items + 1

    index_combinations = [(i, j) for i in range(num_next_pages) for j in range(num_sub_page_items)]
    expected_names = ["root_item", *(f"page_{i}_item_{j}" for i, j in index_combinations)]
    assert [r.name for r in result] == expected_names


def test_sub_folders(list_client, httpx_mock):
    """Test that items in nested sub folders are read correctly"""
    num_sub_folder_layers = 4
    num_sub_folders_per_layer = 3
    num_items_per_sub_folder = 5

    folder_url_base = f"{_api_url}/drives/{library_id}/root:/test_folder"
    httpx_mock.add_response(
        method="GET",
        url=re.compile(f"{folder_url_base}:/children?.+"),
        json={
            "value": [
                {"name": "root_item", "lastModifiedDateTime": "2026-07-13T00:00:00.00Z"},
                *(
                    {
                        "name": f"sub_folder_{i}",
                        "lastModifiedDateTime": "2026-07-13T00:00:00.00Z",
                        "folder": {"childCount": num_items_per_sub_folder + 1},
                    }
                    for i in range(num_sub_folders_per_layer)
                ),
            ],
        },
    )

    def mock_subfolders(folder_path, layer=0):
        for i in range(num_sub_folders_per_layer):
            path_with_next_bit = folder_path + f"/sub_folder_{i}"
            value: list[dict] = [
                {"name": f"item_{layer}_{i}_{j}", "lastModifiedDateTime": "2026-07-13T00:00:00.00Z"}
                for j in range(num_items_per_sub_folder)
            ]

            if layer < num_sub_folder_layers - 1:
                value.extend(
                    (
                        {
                            "name": f"sub_folder_{j}",
                            "lastModifiedDateTime": "2026-07-13T00:00:00.00Z",
                            "folder": {"childCount": num_items_per_sub_folder + 1},
                        }
                        for j in range(num_sub_folders_per_layer)
                    )
                )

            httpx_mock.add_response(
                method="GET",
                url=re.compile(f"{path_with_next_bit}:/children?.+"),
                json={"value": value},
            )

            if layer < num_sub_folder_layers - 1:
                mock_subfolders(path_with_next_bit, layer + 1)

    mock_subfolders(folder_url_base)

    result = list_client.glob("test_folder")

    expected_num_folders = 0
    for ln in range(num_sub_folder_layers):
        expected_num_folders += num_sub_folders_per_layer ** (ln + 1)

    assert len(result) == 1 + expected_num_folders * num_items_per_sub_folder

    paths = [r.path for r in result]
    assert len(set(paths)) == len(paths), "All paths should be unique"
    assert (
        result[-1].name
        == f"item_{num_sub_folder_layers - 1}_{num_sub_folders_per_layer - 1}_{num_items_per_sub_folder - 1}"
    )
