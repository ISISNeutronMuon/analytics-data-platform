"""Microsoft 365 Drive filesystem (fsspec implementation).

Ported from ``dlt_sources/m365/helpers.py`` with the dlt dependency removed.
"""

import datetime
from typing import cast
import urllib.parse as urlparser

from fsspec import AbstractFileSystem
from httpx import Response
from httpx import HTTPStatusError, NetworkError, TimeoutException
import tenacity

from .credentials import M365Credentials

_RETRY_ARGS = {
    "wait": tenacity.wait_exponential(max=5),
    "stop": tenacity.stop_after_attempt(10),
    "reraise": True,
    "retry": (
        tenacity.retry_if_exception_type((NetworkError, TimeoutException))
        | tenacity.retry_if_exception(
            lambda e: isinstance(e, HTTPStatusError) and e.response.status_code >= 500
        )
        | tenacity.retry_if_exception(
            lambda e: isinstance(e, HTTPStatusError) and e.response.status_code == 429
        )
    ),
}


class M365DriveFS(AbstractFileSystem):
    """Minimal, read-only fsspec implementation for M365 Drives."""

    protocol = ("m365",)
    drives_api_url = f"{M365Credentials.api_url}/drives"

    def __init__(self, credentials: M365Credentials, site_url: str, **extra_kwargs):
        super_kwargs = extra_kwargs.copy()
        super_kwargs.pop("use_listings_cache", None)
        super_kwargs.pop("listings_expiry_time", None)
        super_kwargs.pop("max_paths", None)
        super().__init__(**super_kwargs)

        self.client = credentials.create_oauth_client()
        self.client.ensure_active_token(self.client.fetch_token())
        self.drive_url = f"{self.drives_api_url}/{self._get_site_drive_id(site_url)}"

    def fetch_all(self, path: str) -> bytes:
        """Download the full content of a file."""
        return self._msgraph_get(self._path_to_url(path, action="content")).content

    def info(self, path: str, **_) -> dict:
        url = self._path_to_url(path)
        response = self._msgraph_get(url)
        return self._drive_item_info_to_fsspec_info(response.json())

    def ls(self, path: str, detail: bool = True, **kwargs) -> list[dict | str]:
        def _get_page(page_url: str, page_params: dict | None = None):
            response = self._msgraph_get(page_url, params=page_params)
            response_json = response.json()
            return response_json.get("value", []), response_json.get("@odata.nextLink", None)

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

    # ------ private ------

    def _get_site_drive_id(self, site_url: str) -> str:
        def _get_id(endpoint_url: str) -> str:
            response = self._msgraph_get(endpoint_url, params={"$select": "id"})
            response.raise_for_status()
            return response.json()["id"]

        urlparts = urlparser.urlparse(site_url)
        site_id = _get_id(f"{M365Credentials.api_url}/sites/{urlparts.netloc}:{urlparts.path}")
        return _get_id(f"{M365Credentials.api_url}/sites/{site_id}/drive")

    def _drive_item_info_to_fsspec_info(self, drive_item_info: dict) -> dict:
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
        try:
            parent_path = drive_item_info["parentReference"]["path"]
        except KeyError:
            parent_path = ""

        if parent_path:
            try:
                parent_path = "/" + parent_path.split("root:")[1].lstrip("/").rstrip("/")
            except IndexError as exc:
                raise RuntimeError(
                    f"parentReference.path does not contain 'root:' "
                    f"for item {drive_item_info['id']}"
                ) from exc

        return parent_path + "/" + drive_item_info["name"]

    def _path_to_url(self, path: str, action: str | None = None) -> str:
        action = f"/{action}" if action else ""
        path = cast(str, self._strip_protocol(path)).rstrip("/")
        if path and not path.startswith("/"):
            path = "/" + path
        if path:
            path = f":{path}:"
        return f"{self.drive_url}/root{path}{action}"

    def _msgraph_get(self, url: str, **kwargs) -> Response:
        return self._msgraph_request("GET", url, **kwargs)

    @tenacity.retry(**_RETRY_ARGS)
    def _msgraph_request(self, method: str, url: str, **kwargs) -> Response:
        return self.client.request(method, url, **kwargs)
