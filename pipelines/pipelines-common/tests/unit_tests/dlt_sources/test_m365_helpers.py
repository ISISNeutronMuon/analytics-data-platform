from pipelines_common.dlt_sources.m365.helpers import M365CredentialsResource

import pytest
from pytest_httpx import HTTPXMock

from unit_tests.dlt_sources.conftest import SharePointTestSettings


def test_m365_credentials_fetch_oauth_token_raises_Exception_if_error_returned(
    httpx_mock: HTTPXMock,
):
    creds = M365CredentialsResource("tenant_id", "client_id", "client_secret")
    error_code, error_desc = "MS400", "An error occurred while retrieving the token"
    httpx_mock.add_response(
        method="POST",
        url=creds.oauth2_token_endpoint,
        json={"error": error_code, "error_description": error_desc},
    )

    with pytest.raises(Exception) as exc:
        creds.fetch_token()

    assert error_code in str(exc.value)
    assert error_desc in str(exc.value)


def test_m365_credentials_fetch_oauth_token_succeeds_if_no_error_in_response(httpx_mock: HTTPXMock):
    creds = M365CredentialsResource("tenant_id", "client_id", "client_secret")

    httpx_mock.add_response(
        method="POST",
        url=creds.oauth2_token_endpoint,
        json={"access_token": SharePointTestSettings.access_token},
    )

    token = creds.fetch_token()

    assert token["access_token"] == SharePointTestSettings.access_token
