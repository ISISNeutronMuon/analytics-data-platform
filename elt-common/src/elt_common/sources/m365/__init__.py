"""Read files from a SharePoint document library without dlt.

Replaces ``dlt_sources/m365/__init__.py`` — uses ``M365DriveFS`` directly
instead of going through dlt's ``glob_files`` / ``FileItemDict`` machinery.
"""

import dataclasses
import datetime
import fnmatch
import logging
from typing import Sequence

from .credentials import M365Credentials
from .drive import M365DriveFS

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SharePointFile:
    """Lightweight description of a file in a SharePoint document library."""

    file_name: str
    file_url: str
    modification_date: datetime.datetime
    size: int
    _fs: M365DriveFS = dataclasses.field(repr=False)

    def read_bytes(self) -> bytes:
        """Download the full content of the file."""
        return self._fs.fetch_all(self.file_url)


def list_sharepoint_files(
    credentials: M365Credentials,
    site_url: str,
    file_glob: str,
    *,
    modified_after: datetime.datetime | None = None,
) -> list[SharePointFile]:
    """List files in a SharePoint document library matching a glob pattern.

    :param credentials: M365 OAuth2 credentials.
    :param site_url: Absolute URL to the SharePoint site.
    :param file_glob: Glob pattern relative to the library root (e.g. ``/data/**/*.csv``).
    :param modified_after: If set, only return files modified after this timestamp.
    :returns: List of :class:`SharePointFile` objects.
    """
    fs = M365DriveFS(credentials, site_url)
    results: list[SharePointFile] = []

    # Walk the directory tree to find matching files
    files = _glob_drive(fs, file_glob)

    for item in files:
        mtime = item.get("mtime", datetime.datetime.min)
        file_name = item["name"]

        if modified_after and mtime <= modified_after:
            logger.debug(f"Skipping old file: {file_name} (mtime={mtime})")
            continue

        logger.debug(f"Found file: {file_name} (mtime={mtime})")
        results.append(
            SharePointFile(
                file_name=file_name.rsplit("/", 1)[-1],
                file_url=file_name,
                modification_date=mtime,
                size=item.get("size", 0),
                _fs=fs,
            )
        )

    return results


def _glob_drive(fs: M365DriveFS, file_glob: str) -> Sequence[dict]:
    """Glob files from the drive, walking directories as needed.

    Uses fsspec ``ls`` and ``fnmatch`` to match the glob pattern.
    This walks directories segment-by-segment to handle ``**`` and ``*`` patterns.
    """
    # Normalise the glob
    file_glob = file_glob.lstrip("/")

    # Split into directory prefix (before any wildcards) and the pattern
    parts = file_glob.split("/")
    prefix_parts = []
    for part in parts:
        if "*" in part or "?" in part or "[" in part:
            break
        prefix_parts.append(part)

    base_path = "/" + "/".join(prefix_parts) if prefix_parts else "/"
    full_pattern = "/" + file_glob

    # Recursively list and filter
    return _walk_and_match(fs, base_path, full_pattern)


def _walk_and_match(fs: M365DriveFS, path: str, pattern: str) -> list[dict]:
    """Walk directories and return files matching the fnmatch pattern."""
    matches = []
    try:
        items = fs.ls(path, detail=True)
    except Exception:
        return matches

    for item in items:
        name = item.get("name", "")
        if item.get("type") == "directory":
            # Always recurse into directories — fnmatch handles the filtering
            matches.extend(_walk_and_match(fs, name, pattern))
        elif item.get("type") == "file":
            if fnmatch.fnmatch(name, pattern):
                matches.append(item)

    return matches
