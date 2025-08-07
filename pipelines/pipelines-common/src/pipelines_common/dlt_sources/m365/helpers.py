from typing import Any, Dict, ClassVar
import urllib.parse as urlparser

from authlib.integrations.httpx_client import OAuth2Client
from dlt.common.configuration.specs import configspec
from dlt.common.typing import TSecretStrValue
import httpx


@configspec
class M365CredentialsResource:
    """Support configuration of authentication with msal through dlt config mechanism"""

    # Constants
    base_url: ClassVar[str] = "https://graph.microsoft.com"
    api_url: ClassVar[str] = f"{base_url}/v1.0"
    default_scope: ClassVar[str] = f"{base_url}/.default"
    login_url: ClassVar[str] = "https://login.microsoftonline.com"

    # Instance variables
    tenant_id: str = None
    client_id: str = None
    client_secret: TSecretStrValue = None

    @property
    def authority_url(self) -> str:
        return f"{self.login_url}/{self.tenant_id}"

    @property
    def oauth2_token_endpoint(self) -> str:
        return f"{self.authority_url}/oauth2/v2.0/token"

    def fetch_token(self) -> Dict[str, Any]:
        """Acquire tokens via OAuth. Raises an Exception if an error occurs"""
        oauth_client = OAuth2Client(
            self.client_id,
            self.client_secret,
            token_endpoint=self.oauth2_token_endpoint,
            scope=self.default_scope,
        )
        return oauth_client.fetch_token()

    def oauth2_client_params(self, token: Dict[str, Any]):
        """Return a set of parameters suitable for initializing an OAuth client"""
        return {
            "scope": " ".join(self.default_scope),
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "token": token,
            "token_endpoint": self.oauth2_token_endpoint,
        }


def get_site_drive_id(site_url: str, access_token: str) -> str:
    """Get the SharePoint site library drive id from the url

    :param site_url: Full url of the main page of the SharePoint site
    :param access_token: A Bearer access token to access the resource
    :return: The id property of the site's drive
    :raises: A requests.exception if an error code is encountered
    """

    # Two steps needed:
    #  - get the site ID from the path
    #  - get the drive ID from the /drive resource of the site
    def _get_id(endpoint_url: str) -> str:
        params = {"$select": "id"}
        response = httpx.get(
            endpoint_url,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
        )
        response.raise_for_status()
        return response.json()["id"]

    urlparts = urlparser.urlparse(site_url)
    site_id = _get_id(f"{M365CredentialsResource.api_url}/sites/{urlparts.netloc}:{urlparts.path}")
    return _get_id(f"{M365CredentialsResource.api_url}/sites/{site_id}/drive")
