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
        self._client = Github(auth=Auth.Token(cfg.access_token))

    def extract_resource_properties(self):
        yield (
            "repositories",
            ResourceProperties(
                extractor=self.extract_repositories,
                write_properties=ResourceWriteProperties(write_mode="replace"),
            ),
        )

    def extract_repositories(self, _: Watermark | None) -> Iterator[pa.Table]:
        repos = []

        organization = self._client.get_organization("ISISNeutronMuon")

        for repo in organization.get_repos():
            license = repo.license
            license_name: str | None
            if license is None:
                license_name = None
            else:
                license_name = license.name

            repos.append(
                {
                    "name": repo.name,
                    "owner": repo.owner.login,
                    "public": not repo.private,
                    "fork": repo.fork,
                    "default_branch": repo.default_branch,
                    "size_kilobytes": repo.size,
                    "license": license_name,
                }
            )

        repos_schema = pa.schema(
            [
                pa.field("name", pa.string()),
                pa.field("owner", pa.string()),
                pa.field("public", pa.bool_()),
                pa.field("fork", pa.bool_()),
                pa.field("default_branch", pa.string()),
                pa.field("size_kilobytes", pa.int64()),
                pa.field("license", pa.string()),
            ]
        )
        repos_table = pa.Table.from_pylist(repos, schema=repos_schema)
        yield repos_table
