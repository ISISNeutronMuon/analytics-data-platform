from typing import Iterator

from elt_common.extract import (
    BaseExtract,
    ResourceProperties,
    ResourceWriteProperties,
    Watermark,
)
from github import Github
from github import Auth
from pydantic_settings import BaseSettings
import pyarrow as pa


class GitHubCredentials(BaseSettings):
    url: str
    access_token: str


class Extract(BaseExtract[GitHubCredentials]):
    config_cls = GitHubCredentials

    def __init__(self, cfg: GitHubCredentials):
        super().__init__(cfg)
        self._client = Github(
            auth=Auth.Token(cfg.access_token),
            base_url=f"{cfg.url}",  # /api/v3
        )
        # self._client = Github(auth=Auth.Token(cfg.access_token))

    def extract_resource_properties(self):
        yield (
            "repositories",
            ResourceProperties(
                extractor=self.extract_repositories,
                write_properties=ResourceWriteProperties(write_mode="replace"),
            ),
        )

    # Create a table with the following columns:
    # - name: The name of the repository (string)
    # - owner: The owner of the repository (string)
    # - public: Flag indicating if public (bool)
    # - fork: Flag indicating if this is a fork (bool)
    # - default_branch: The name of the default branch (string)
    # - size_kilobytes: The size of the repository (integer)
    def extract_repositories(self, _: Watermark | None) -> Iterator[pa.Table]:
        repos = []

        for repo in self._client.get_repos():
            repos.append(
                {
                    "name": repo.name,
                    "owner": repo.owner,
                    "public": True,  # look into this. Placeholder
                    "fork": repo.fork,
                    "default_branch": repo.default_branch,
                    "size": repo.size,
                }
            )

        repos_schema = pa.schema(
            [
                pa.field("name", pa.string()),
                pa.field("owner", pa.string()),
                pa.field("public", pa.bool()),
                pa.field("fork", pa.bool()),
                pa.field("default_branch", pa.string()),
                pa.field("size", pa.int()),
            ]
        )
        repos_table = pa.Table.from_pylist(repos, schema=repos_schema)
        yield repos_table
        # pass
