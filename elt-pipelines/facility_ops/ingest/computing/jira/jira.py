import os

from atlassian import Jira
from elt_common.extract import BaseExtract, ResourceProperties
import requests
import datetime as dt

import pyarrow as pa

ISIS_PROJECT_PREFIX = "[ISIS]"
FIELDS_TO_EXTRACT = (
    "issueKey,issuetype,status,priority,created,updated,customfield_10591"
)


class Extract(BaseExtract):
    def __init__(self, config):
        super().__init__(config)

    def extract_resource_properties(self):
        yield (
            ("", ResourceProperties(extractor=self.extract_jira_issues)),
            ("", ResourceProperties(extractor=self.extract_time_spent_in_status)),
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


def extract_project_names_starting_with(project_prefix: str) -> list[str]:
    jira_cloud = jira_connection()

    projects = jira_cloud.get_all_projects()

    project_names = []
    for project in projects:
        if project["name"] not in project_names:
            project_names.append(project["name"])

    return list(filter(lambda p: p.startswith(project_prefix), project_names))


def extract_jira_issues() -> pa.Table:
    project_names = extract_project_names_starting_with(ISIS_PROJECT_PREFIX)

    jira_cloud = jira_connection()

    issues = []
    for project_name in project_names:
        project_issues = jira_cloud.get_all_project_issues(
            project_name, fields=FIELDS_TO_EXTRACT
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


def extract_time_spent_in_status() -> pa.Table:
    project_names = extract_project_names_starting_with(ISIS_PROJECT_PREFIX)

    jira_cloud = jira_connection()

    issues = []

    for project_name in project_names:
        project_issues = jira_cloud.get_all_project_issues(
            project_name, fields=FIELDS_TO_EXTRACT
        )

        for project_issue in project_issues:
            issue_status_changelog = jira_cloud.get_issue_status_changelog(
                project_issue["key"]
            )

            duration_of_status = {}

            DATE_FORMAT_STRING = "%Y-%m-%dT%H:%M:%S.%f%z"

            if len(issue_status_changelog) == 0:
                duration_of_status[project_issue["fields"]["status"]["name"]] = (
                    dt.datetime.now(dt.timezone.utc)
                    - dt.datetime.strptime(
                        project_issue["fields"]["created"], DATE_FORMAT_STRING
                    )
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
                                project_issue["fields"]["created"], DATE_FORMAT_STRING
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
                "project_name": f"{project_name}",
                "issue_key": f"{project_issue['key']}",
                "current_status": f"{project_issue['fields']['status']['name']}",
                "created": f"{project_issue['fields']['created']}",
                "updated": f"{project_issue['fields']['updated']}",
                "duration_of_status": f"{duration_of_status}",
            }
            issues.append(issue_status)

    issues_table = pa.Table.from_pylist(issues)
    print(issues_table)
    return issues_table
