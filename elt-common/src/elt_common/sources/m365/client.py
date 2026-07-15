import datetime as dt
import fnmatch
import logging
import re
import urllib.parse as urlparser
from dataclasses import dataclass
from typing import Literal

import tenacity
from authlib.integrations.httpx_client import OAuth2Client
from httpx import HTTPStatusError, NetworkError, Response, TimeoutException

from elt_common.sources.m365.configuration import M365Config, base_url

LOGGER = logging.getLogger(__name__)

_RETRY_ARGS = {
    "wait": tenacity.wait_exponential(max=5),
    "stop": tenacity.stop_after_attempt(5),
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

_api_url = f"{base_url}/v1.0"
_drives_api_url = f"{_api_url}/drives"


@dataclass
class M365File:
    name: str
    path: str
    last_modified: dt.datetime

    @classmethod
    def from_sp_file(cls, file, path):
        return M365File(
            name=file["name"],
            path=f"{path}/{file['name']}",
            last_modified=dt.datetime.fromisoformat(file["lastModifiedDateTime"]),
        )


class SPListClient:
    """Client for searching for and downloading files from an M365 SharePoint list"""

    def __init__(self, config: M365Config):
        self.client: OAuth2Client = config.credentials.create_oauth_client()
        self.client.ensure_active_token(self.client.fetch_token())
        self.drive_url = f"{_drives_api_url}/{self._get_site_drive_id(config.site_url)}"

    def read_file(self, path: str):
        """Retrieve the contents of a file"""
        url = self._path_to_url(path, action="content")
        return self._msgraph_get(url).content

    def glob(
        self, root: str, pattern: str | None = None, modified_after: dt.datetime | None = None
    ) -> list[M365File]:
        """Get file info for all files under a root directory, optionally filtered by a glob pattern and/or a last modified date.

        :param root: the root path to search under.
        :param pattern: if given, only files with paths matching the pattern are returned
        :param modified_after: if given, only files modified after this datetime are returned. Must be tz aware.
        """
        if "*" in root:
            raise ValueError(
                "The 'root' parameter for 'glob' must be a path to a directory, "
                "not a glob pattern. To apply a glob filter to files under the "
                "'root' directory, use the 'pattern' parameter."
            )

        files = self._read_tree(root)
        if pattern:
            regex = fnmatch.translate(pattern)
            matcher = re.compile(regex)
            files = filter(lambda f: matcher.match(f.path) is not None, files)

        if modified_after is not None:
            files = filter(lambda m: m.last_modified > modified_after, files)

        return list(files)

    def _read_tree(
        self,
        path: str,
    ) -> list[M365File]:
        """Gets file info for all files under a directory recursively"""

        def _get_page(page_url: str, page_params: dict | None = None) -> tuple[list, str | None]:
            response = self._msgraph_get(page_url, params=page_params)
            response_json = response.json()
            return response_json.get("value", []), response_json.get("@odata.nextLink", None)

        url = self._path_to_url(path, action="children")
        params = {"$select": "name,lastModifiedDateTime,folder"}
        items, next_page = _get_page(url, params)
        if items:
            while next_page is not None:
                page_items, next_page = _get_page(next_page)
                items.extend(page_items)
        else:
            LOGGER.warning(
                f"_read_tree was called for {path}, which had no children. "
                "Could be a file, or an empty directory"
            )

            return []

        file_items = [i for i in items if "folder" not in i]
        files = [M365File.from_sp_file(f, path) for f in file_items]
        folders = [i for i in items if "folder" in i and i["folder"]["childCount"] > 0]
        for folder in folders:
            files.extend(self._read_tree(f"{path}/{folder['name']}"))

        return files

    def _path_to_url(self, path: str, action: Literal["children", "content"] | None = None) -> str:
        """Constructs the URL for a drive item from the path to a site item

        :param path: Path to the site item
        :param action: A string denoting an action
        """
        path = _strip_protocol(path).rstrip("/")
        if path and not path.startswith("/"):
            path = "/" + path
        if path:
            path = f":{path}:"

        action = f"/{action}" if action else ""
        return f"{self.drive_url}/root{path}{action}"

    def _get_site_drive_id(self, site_url: str) -> str:
        """Get the drive id for the SP site's URL

        :param site_url: Full url of the main page of the SharePoint site
        :return: The id property of the site's drive
        """

        def _get_id(endpoint_url: str) -> str:
            response = self._msgraph_get(endpoint_url, params={"$select": "id"})
            return response.json()["id"]

        # First get the id of the site from the graph API
        url_parts = urlparser.urlparse(site_url)
        graph_url = f"{_api_url}/sites/{url_parts.netloc}:{url_parts.path}"
        site_id = _get_id(graph_url)

        # Then get the id of the site's drive
        drive_url = f"{_api_url}/sites/{site_id}/drive"
        return _get_id(drive_url)

    @tenacity.retry(**_RETRY_ARGS)
    def _msgraph_get(self, url: str, **kwargs) -> Response:
        response = self.client.request("GET", url, **kwargs)
        response.raise_for_status()
        return response


def _strip_protocol(path: str):
    return path.split("://", 1)[-1]
