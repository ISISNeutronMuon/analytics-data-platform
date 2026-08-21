import os

from atlassian import Jira
from elt_common.extract import BaseExtract, ResourceProperties
import requests

import pyarrow as pa


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


def jira_connection(
    url: str | None = None,
    email: str | None = None,
    token: str | None = None,
    cloud: bool = True,
) -> Jira:
    url = _value_or_env_variable(url, "JIRA_URL")
    email = _value_or_env_variable(email, "EMAIL_ADDRESS")
    token = _value_or_env_variable(token, "ATLASSIAN_API_TOKEN")

    session = requests.Session()
    jira_connection = Jira(url, email, token, session=session, cloud=cloud)
    return jira_connection


def extract_jira_issues():
    jira_cloud = jira_connection()

    projects = jira_cloud.get_all_projects()

    project_names = []
    for project in projects:
        if project["name"] not in project_names:
            project_names.append(project["name"])

    prefix = "[ISIS]"
    project_names = list(filter(lambda p: p.startswith(prefix), project_names))

    issues = []
    for project_name in project_names:
        project_issues = jira_cloud.get_all_project_issues(
            project_name,
            fields="issueKey,issuetype,status,priority,created,updated,customfield_10591",
        )

        for project_issue in project_issues:
            issues.append(
                {
                    "project_name": f"{project_name}",
                    "issue_key": f"{project_issue['key']}",
                    "issue_type": f"{project_issue['fields']['issuetype']}",
                    "status": f"{project_issue['fields']['status']}",
                    "priority": f"{project_issue['fields']['priority']}",
                    "created": f"{project_issue['fields']['created']}",
                    "updated": f"{project_issue['fields']['updated']}",
                    "teams": f"{project_issue['fields'].get('customfield_10591')}",
                }
            )

    issues_table = pa.Table.from_pylist(issues)
    return issues_table


if __name__ == "__main__":
    extract_jira_issues()
