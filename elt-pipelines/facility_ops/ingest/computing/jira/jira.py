import enum
import os

from atlassian import Jira
from elt_common.extract import BaseExtract, ResourceProperties
import requests
import datetime as dt

import pyarrow as pa


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
            (
                "time_spent_in_status_per_issue",
                ResourceProperties(extractor=self.extract_time_spent_in_status),
            ),
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

            issues.append(
                {
                    "project_name": project_name,
                    "issue_key": project_issue[IssueField.IssueKey],
                    "issue_type": fields[IssueField.IssueType],
                    "status": fields[IssueField.Status],
                    "priority": fields[IssueField.Priority],
                    "created": fields[IssueField.Created],
                    "updated": fields[IssueField.Updated],
                    "teams": fields.get(IssueField.Teams),
                }
            )

    issues_table = pa.Table.from_pylist(issues)
    return issues_table


def extract_time_spent_in_status() -> pa.Table:
    project_names = extract_isis_project_names()

    jira_cloud = jira_connection()

    issues = []

    for project_name in project_names:
        project_issues = jira_cloud.get_all_project_issues(
            project_name, fields=[field.value for field in IssueField]
        )

        for project_issue in project_issues:
            issue_status_changelog = jira_cloud.get_issue_status_changelog(
                project_issue[IssueField.IssueKey]
            )

            fields = project_issue["fields"]

            duration_of_status = {}

            DATE_FORMAT_STRING = "%Y-%m-%dT%H:%M:%S.%f%z"

            if len(issue_status_changelog) == 0:
                duration_of_status[
                    project_issue["fields"][IssueField.Status]["name"]
                ] = dt.datetime.now(dt.timezone.utc) - dt.datetime.strptime(
                    project_issue["fields"][IssueField.Created], DATE_FORMAT_STRING
                )
            else:
                # reverse array as the data are in reverse chronological order
                for i in range(len(issue_status_changelog), -1, -1):
                    if i == len(issue_status_changelog):
                        duration_of_status[issue_status_changelog[-1]["from"]] = (
                            dt.datetime.strptime(
                                issue_status_changelog[-1]["date"], DATE_FORMAT_STRING
                            )
                            - dt.datetime.strptime(
                                project_issue["fields"][IssueField.Created],
                                DATE_FORMAT_STRING,
                            )
                        )
                    elif i == 0:
                        duration_of_status[issue_status_changelog[0]["to"]] = (
                            dt.datetime.now(dt.timezone.utc)
                            - dt.datetime.strptime(
                                issue_status_changelog[0]["date"], DATE_FORMAT_STRING
                            )
                        )
                    elif issue_status_changelog[i - 1] == issue_status_changelog[i]:
                        duration_of_status[issue_status_changelog[i]["to"]] += (
                            dt.datetime.strptime(
                                issue_status_changelog[i + 1]["date"],
                                DATE_FORMAT_STRING,
                            )
                            - dt.datetime.strptime(
                                issue_status_changelog[i]["date"], DATE_FORMAT_STRING
                            )
                        )

            issue_status = {
                "project_name": project_name,
                "issue_key": project_issue[IssueField.IssueKey],
                "current_status": fields[IssueField.Status]["name"],
                "created": fields[IssueField.Created],
                "updated": fields[IssueField.Updated],
                "duration_of_status": duration_of_status,
            }
            issues.append(issue_status)

    issues_table = pa.Table.from_pylist(issues)
    print(issues_table)
    return issues_table
