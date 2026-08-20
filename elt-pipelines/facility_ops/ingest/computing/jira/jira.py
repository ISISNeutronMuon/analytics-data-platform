import os

from atlassian import Jira
from elt_common.extract import BaseExtract, ResourceProperties
import requests


class JiraCredentials:
    jira_url: str
    jira_username: str
    jira_api_token: str

    def create_client(self):
        pass


class Extract(BaseExtract):
    def __init__(self, config):
        super().__init__(config)

    def extract_resource_properties(self):
        yield ("", ResourceProperties(extractor=self.extract_jira_issue))


def _value_or_env_variable(value: str | None, env_var_name: str) -> str:
    if value is not None:
        return value
    else:
        try:
            return os.environ[env_var_name]
        except KeyError:
            raise KeyError(f"Environment variable '{env_var_name}' not found.")


def extract_jira_issues(
    url: str | None = None, email: str | None = None, token: str | None = None
):
    url = _value_or_env_variable(url, "JIRA_URL")
    email = _value_or_env_variable(email, "EMAIL_ADDRESS")
    token = _value_or_env_variable(token, "ATLASSIAN_API_TOKEN")

    session = requests.Session()
    jira_cloud = Jira(url, email, token, session=session, cloud=True)

    projects = jira_cloud.get_all_projects()

    project_names = []
    for project in projects:
        if project["name"] not in project_names:
            project_names.append(project["name"])

    prefix = "[ISIS]"
    project_names = list(filter(lambda p: p.startswith(prefix), project_names))

    issues = []
    for name in project_names:
        issues.append(jira_cloud.get_all_project_issues(name))

    print(issues)

    return issues


if __name__ == "__main__":
    extract_jira_issues()
