import enum
import os

from atlassian import Jira
from elt_common.extract import BaseExtract, ResourceProperties
import requests

import pyarrow as pa
import datetime as dt

DATE_FORMAT_STRING = "%Y-%m-%dT%H:%M:%S.%f%z"


class IssueField(enum.StrEnum):
    IssueKey = "key"
    IssueType = "issuetype"
    Status = "status"
    Priority = "priority"
    Created = "created"
    Updated = "updated"
    Teams = "customfield_10591"


class Extract(BaseExtract):
    def extract_resource_properties(self):
        yield (
            ("all_jira_issues", ResourceProperties(extractor=self.extract_jira_issues)),
        )


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


def extract_isis_project_names() -> list[str]:
    jira_cloud = jira_connection()

    projects = jira_cloud.get_all_projects()

    project_names = []
    for project in projects:
        if project["name"] not in project_names:
            project_names.append(project["name"])

    return list(filter(lambda p: p.startswith("[ISIS]"), project_names))


def extract_jira_issues() -> pa.Table:
    project_names = extract_isis_project_names()

    jira_cloud = jira_connection()

    issues = []
    for project_name in project_names:
        project_issues = jira_cloud.get_all_project_issues(
            project_name, fields=[field.value for field in IssueField]
        )

        for project_issue in project_issues:
            fields = project_issue["fields"]

            created = dt.datetime.strptime(
                fields[IssueField.Created], DATE_FORMAT_STRING
            )
            updated = dt.datetime.strptime(
                fields[IssueField.Updated], DATE_FORMAT_STRING
            )

            teams = fields.get(IssueField.Teams)
            team_names = []
            if teams is not None:
                for team in teams:
                    if team["value"] not in team_names:
                        team_names.append(team["value"])

            issues.append(
                {
                    "project_name": project_name,
                    "issue_key": project_issue[IssueField.IssueKey],
                    "issue_type": fields[IssueField.IssueType]["name"],
                    "status": fields[IssueField.Status]["name"],
                    "priority": fields[IssueField.Priority]["name"],
                    "created": created,
                    "updated": updated,
                    "teams": team_names,
                }
            )

    issues_table = pa.Table.from_pylist(issues)
    return issues_table


if __name__ == "__main__":
    extract_jira_issues()
