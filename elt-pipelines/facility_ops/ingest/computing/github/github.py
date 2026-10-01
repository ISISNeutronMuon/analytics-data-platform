from elt_common.extract import BaseExtract, ResourceProperties, ResourceWriteProperties
from pydantic_settings import BaseSettings
from github import Github
from github import Auth


class GitHubCredentials(BaseSettings):
    url: str
    access_token: str


class Extract(BaseExtract[GitHubCredentials]):
    def __init__(self, config):
        super().__init__(config)
        self._client = Github(
            auth=Auth.Token(self.access_token), base_url=f"{self.url}/api/v3"
        )

    def extract_resource_properties(self):
        yield (
            "github_repository_information",
            ResourceProperties(
                extractor=self.extract_repository_information,
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
    def extract_repository_information(self):
        pass
