"""Reads files from a SharePoint documents library"""

from typing import Iterator, List

import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict, glob_files, MTIME_DISPATCH
from dlt.extract import decorators
from msgraphfs import MSGDriveFS
import pendulum

from .helpers import M365CredentialsResource, get_site_drive_id
from .settings import DEFAULT_CHUNK_SIZE

# Add our MSGDriveFS protocol to the known modificaton time mappings
MSGDRIVEFS_PROTOCOL = MSGDriveFS.protocol[0]
MTIME_DISPATCH[MSGDRIVEFS_PROTOCOL] = MTIME_DISPATCH["file"]


# This is designed to look similar to the dlt.filesystem resource where the resource returns DriveItem
# objects that include the content as raw bytes. The bytes need to be parsed by an appropriate
# transformer
@decorators.resource()
def sharepoint(
    site_url: str = dlt.config.value,
    credentials: M365CredentialsResource = dlt.secrets.value,
    file_glob: str = dlt.config.value,
    files_per_page: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
    modified_after: pendulum.DateTime | None = None,
) -> Iterator[List[FileItemDict]]:
    """A dlt resource to pull files stored in a SharePoint document library.

    :param site_url: The absolute url to the main page of the SharePoint site
    :param file_glob: A glob pattern, relative to the site's document library root.
                       For example, if a file called 'file_to_ingest.csv' exists in the "Documents"
                       library in folder 'incoming' then the file_path would be '/incoming/file_to_ingest.csv'
    :return: List[DltResource]: A list of DriveItems representing the file
    """
    oauth_token = credentials.fetch_token()
    access_token = oauth_token["access_token"]
    sp_library = MSGDriveFS(
        drive_id=get_site_drive_id(site_url, access_token),
        oauth2_client_params=credentials.oauth2_client_params(oauth_token),
    )

    files_chunk: List[FileItemDict] = []
    for file_model in glob_files(
        sp_library, bucket_url=f"{MSGDRIVEFS_PROTOCOL}://", file_glob=file_glob
    ):
        if modified_after and file_model["modification_date"] <= modified_after:
            continue

        file_dict = FileItemDict(file_model, fsspec=sp_library)
        if extract_content:
            file_dict["file_content"] = file_dict.read_bytes()
        files_chunk.append(file_dict)

        # wait for the chunk to be full
        if len(files_chunk) >= files_per_page:
            yield files_chunk
            files_chunk = []

    if files_chunk:
        yield files_chunk
