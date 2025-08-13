from typing import ClassVar, cast
import urllib.parse as urlparser

from authlib.integrations.httpx_client import OAuth2Client
from fsspec import AbstractFileSystem
from dlt.common.configuration.specs import configspec
from dlt.common.typing import TSecretStrValue
from httpx import Response
import datetime


@configspec
class M365CredentialsResource:
    """Supports configuring m365 credentials through dlt secrets"""

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

    def create_oauth_client(self) -> OAuth2Client:
        return OAuth2Client(
            self.client_id,
            self.client_secret,
            token_endpoint=self.oauth2_token_endpoint,
            scope=self.default_scope,
            grant_type="client_credentials",
            follow_redirects=True,
        )


class M365DriveFS(AbstractFileSystem):
    """Implements a minimal, read-only implementation of fsspec for M365 Drives."""

    protocol = ("m365",)
    drives_api_url = f"{M365CredentialsResource.api_url}/drives"
    http_retries = 5

    def __init__(self, credentials: M365CredentialsResource, site_url: str, **extra_kwargs):
        super_kwargs = extra_kwargs.copy()
        super_kwargs.pop("use_listings_cache", None)
        super_kwargs.pop("listings_expiry_time", None)
        super_kwargs.pop("max_paths", None)
        # passed to fsspec superclass... we don't support directory caching
        super().__init__(**super_kwargs)

        self.client: OAuth2Client = credentials.create_oauth_client()
        self.client.ensure_active_token(self.client.fetch_token())
        self.drive_url = f"{self.drives_api_url}/{self._get_site_drive_id(site_url)}"

    def fetch_all(self, path: str) -> bytes:
        """Given a path to a drive item, including the protocol string, fetch
        the content of the file as bytes.
        """
        return self._msgraph_get(self._path_to_url(path, action="content")).content

    def info(self, path: str, **_) -> dict:
        """Get information about a file or directory.

        Parameters
        ----------
        path : str
            Path to get information about
        """
        url = self._path_to_url(path)
        response = self._msgraph_get(url)
        return self._drive_item_info_to_fsspec_info(response.json())

    def ls(
        self,
        path: str,
        detail: bool = True,
        **kwargs,
    ) -> list[dict | str]:
        """List files in the given path.

        Parameters
        ----------
        path : str
            Path to list files in
        detail: bool
            if True, gives a list of dictionaries, where each is the same as
            the result of ``info(path)``. If False, gives a list of paths
            (str).
        kwargs: may have additional backend-specific options, such as version
            information
        """

        def _get_page(page_url: str, page_params: dict | None = None) -> tuple[list, str | None]:
            response = self._msgraph_get(page_url, params=page_params)
            response_json = response.json()
            return response_json.get("value", []), (response_json.get("@odata.nextLink", None))

        url = self._path_to_url(path, action="children")
        params = None if detail else {"$select": "name,parentReference"}

        items, next_page = _get_page(url, params)
        if items:
            while next_page is not None:
                page_items, next_page = _get_page(next_page)
                items.extend(page_items)
        else:
            try:
                item = self.info(path, **kwargs)
                if item["type"] == "file":
                    items = [item["item_info"]]
            except FileNotFoundError:
                pass

        if detail:
            return [self._drive_item_info_to_fsspec_info(item) for item in items]
        else:
            return [self._get_path(item) for item in items]

    ###### private
    def _get_site_drive_id(self, site_url: str) -> str:
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
            response = self._msgraph_get(endpoint_url, params={"$select": "id"})
            response.raise_for_status()
            return response.json()["id"]

        urlparts = urlparser.urlparse(site_url)
        site_id = _get_id(
            f"{M365CredentialsResource.api_url}/sites/{urlparts.netloc}:{urlparts.path}"
        )
        return _get_id(f"{M365CredentialsResource.api_url}/sites/{site_id}/drive")

    def _drive_item_info_to_fsspec_info(self, drive_item_info: dict) -> dict:
        """Convert a drive item info to a fsspec info dictionary.

        See
        https://docs.microsoft.com/en-us/graph/api/resources/driveitem?view=graph-rest-1.0
        """
        _type = "other"
        if drive_item_info.get("folder"):
            _type = "directory"
        elif drive_item_info.get("file"):
            _type = "file"
        data = {
            "name": self._get_path(drive_item_info),
            "size": drive_item_info.get("size", 0),
            "type": _type,
            "item_info": drive_item_info,
            "time": datetime.datetime.fromisoformat(
                drive_item_info.get("createdDateTime", "1970-01-01T00:00:00Z")
            ),
            "mtime": datetime.datetime.fromisoformat(
                drive_item_info.get("lastModifiedDateTime", "1970-01-01T00:00:00Z")
            ),
            "id": drive_item_info.get("id"),
        }
        if _type == "file":
            data["mimetype"] = drive_item_info.get("file", {}).get("mimeType", "")
        return data

    def _get_path(self, drive_item_info: dict) -> str:
        """Get a path to the item as if this were a filesystem

        :param drive_item_info: A dict describing a single drive item
        """
        try:
            parent_path = drive_item_info["parentReference"]["path"]
        except KeyError:
            parent_path = ""

        if parent_path:
            # parentRef includes prefix to describe root folder, strip it
            try:
                parent_path = "/" + parent_path.split("root:")[1].lstrip("/").rstrip("/")
            except IndexError as exc:
                raise RuntimeError(
                    f"parentReference.path does not contain 'root:' for item {drive_item_info['id']}"
                ) from exc

        return parent_path + "/" + drive_item_info["name"]

    def _path_to_url(self, path: str, action: str | None = None) -> str:
        """Given a drive item path create a url to access the item.

        :param path: Path to the drive item
        :param action: A string denoting an action such as children
        """
        action = f"/{action}" if action else ""
        path = cast(str, self._strip_protocol(path)).rstrip("/")
        if path and not path.startswith("/"):
            path = "/" + path
        if path:
            path = f":{path}:"

        return f"{self.drive_url}/root{path}{action}"

    def _msgraph_get(self, url: str, **kwargs) -> Response:
        return self._msgraph_request("GET", url, **kwargs)

    def _msgraph_request(self, method: str, url: str, **kwargs) -> Response:
        from retry.api import retry_call

        return retry_call(
            self.client.request,
            fargs=(method, url),
            fkwargs=kwargs,
            tries=self.http_retries,
            backoff=1.7,
            max_delay=15,
        )
