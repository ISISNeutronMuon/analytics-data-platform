"""Reads files from a SharePoint documents library"""

from typing import Iterator, List

import dlt
from dlt.common.storages.fsspec_filesystem import MTIME_DISPATCH, glob_files
from dlt.extract import decorators
import dlt.common.logger as logger
import pendulum

from .helpers import M365CredentialsResource, M365DriveFS, M365DriveItem
from .settings import DEFAULT_CHUNK_SIZE

# Add our M365DriveFS protocol(s) to the known modificaton time mappings
for protocol in M365DriveFS.protocol:
    MTIME_DISPATCH[protocol] = MTIME_DISPATCH["file"]


# This is designed to look similar to the dlt.filesystem resource where the resource returns DriveItem
# objects that include the content as raw bytes. The bytes need to be parsed by an appropriate
# transformer
@decorators.resource
def sharepoint(
    site_url: str = dlt.config.value,
    credentials: M365CredentialsResource = dlt.secrets.value,
    file_glob: str = dlt.config.value,
    files_per_page: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
    modified_after: pendulum.DateTime | None = None,
) -> Iterator[List[M365DriveItem]]:
    """A dlt resource to pull files stored in a SharePoint document library.

    :param site_url: The absolute url to the main page of the SharePoint site
    :param file_glob: A glob pattern, relative to the site's document library root.
                       For example, if a file called 'file_to_ingest.csv' exists in the "Documents"
                       library in folder 'incoming' then the file_path would be '/incoming/file_to_ingest.csv'
    :return: List[DltResource]: A list of DriveItems representing the file
    """
    sp_library = M365DriveFS(credentials, site_url)
    files_chunk: List[M365DriveItem] = []
    for file_model in glob_files(
        sp_library, bucket_url=M365DriveFS.protocol[0] + "://", file_glob=file_glob
    ):
        log_msg = f"Found '{file_model['file_name']}' with modification date '{file_model['modification_date']}'"  # type: ignore
        if modified_after and file_model["modification_date"] <= modified_after:  # type: ignore
            log_msg += ": skipped old item."
            continue
        else:
            log_msg += ": added for processing."
            file_dict = M365DriveItem(file_model, fsspec=sp_library)
            if extract_content:
                file_dict["file_content"] = file_dict.read_bytes()
            files_chunk.append(file_dict)
        logger.debug(log_msg)

        # wait for the chunk to be full
        if len(files_chunk) >= files_per_page:
            yield files_chunk
            files_chunk = []

    if files_chunk:
        yield files_chunk
