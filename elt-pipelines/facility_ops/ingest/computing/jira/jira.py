import enum
from typing import Iterator

from atlassian import JiraCloud
from elt_common.extract import (
    BaseExtract,
    ResourceProperties,
    ResourceWriteProperties,
    Watermark,
)
from pydantic_settings import BaseSettings


import pyarrow as pa
import datetime as dt

DATE_FORMAT_STRING = "%Y-%m-%dT%H:%M:%S.%f%z"


class IssueField(enum.StrEnum):
    Id = "id"
    IssueId = "issueId"
    IssueKey = "key"
    IssueType = "issuetype"
    Status = "status"
    Priority = "priority"
    Created = "created"
    Updated = "updated"
    Teams = "customfield_10591"


class AtlassianCredentials(BaseSettings):
    url: str
    email_address: str
    api_token: str
    cloud: bool = True


class Extract(BaseExtract[AtlassianCredentials]):
    config_cls = AtlassianCredentials

    def __init__(self, cfg: AtlassianCredentials):
        super().__init__(cfg)
        self._issue_keys: dict[int, str] = {}
        self._client = JiraCloud(cfg.url, cfg.email_address, cfg.api_token)

    def extract_resource_properties(self) -> Iterator[tuple[str, ResourceProperties]]:
        yield (
            "isis_jira_issues",
            ResourceProperties(
                extractor=self.extract_isis_jira_issues,
                write_properties=ResourceWriteProperties(write_mode="replace"),
            ),
        )
        yield (
            "issue_status_changelog",
            ResourceProperties(
                extractor=self.extract_issue_status_changelogs,
                write_properties=ResourceWriteProperties(write_mode="replace"),
            ),
        )

    def extract_isis_jira_issues(self, _: Watermark | None) -> Iterator[pa.Table]:
        project_names = self.get_isis_project_names()

        issues = []
        for project_name in project_names:
            jql = f'project = "{project_name}" ORDER BY key'
            project_issues = self._client.enhanced_jql_get_list_of_tickets(
                jql, fields=[field.value for field in IssueField]
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
                team_names = (
                    [team["value"] for team in teams] if teams is not None else None
                )

                self._issue_keys[project_issue[IssueField.Id.value]] = project_issue[
                    IssueField.IssueKey.value
                ]

                issues.append(
                    {
                        "issue_key": project_issue[IssueField.IssueKey],
                        "issue_type": fields[IssueField.IssueType]["name"],
                        "project_name": project_name,
                        "status": fields[IssueField.Status]["name"],
                        "priority": fields[IssueField.Priority]["name"],
                        "created": created,
                        "updated": updated,
                        "teams": team_names,
                    }
                )

        issues_schema = pa.schema(
            [
                pa.field("issue_key", pa.string()),
                pa.field("issue_type", pa.string()),
                pa.field("project_name", pa.string()),
                pa.field("status", pa.string()),
                pa.field("priority", pa.string()),
                pa.field("created", pa.timestamp("s")),
                pa.field("updated", pa.timestamp("s")),
                pa.field("teams", pa.list_(pa.string())),
            ]
        )

        issues_table = pa.Table.from_pylist(issues, schema=issues_schema)
        yield issues_table

    def extract_issue_status_changelogs(self, _: Watermark | None):
        issue_changelogs = []

        payload = {
            "fieldIds": [IssueField.Status.value],
            "issueIdsOrKeys": list(self._issue_keys.values()),
        }

        raw_changelog = self._client.get_bulk_changelogs(payload)
        issue_changelogs = raw_changelog["issueChangeLogs"]

        changes = []

        for issue in issue_changelogs:
            for changeHistory in issue["changeHistories"]:
                # conversion to milliseconds
                changed_at = dt.datetime.fromtimestamp(
                    changeHistory[IssueField.Created] / 1000, dt.timezone.utc
                )

                for change in changeHistory["items"]:
                    changes.append(
                        {
                            "issue_key": self._issue_keys[issue[IssueField.IssueId]],
                            "from_status": change["fromString"],
                            "to_status": change["toString"],
                            "changed_at": changed_at,
                        }
                    )

        issue_status_changelog_schema = pa.schema(
            [
                pa.field("issue_key", pa.string()),
                pa.field("from_status", pa.string()),
                pa.field("to_status", pa.string()),
                pa.field("changed_at", pa.timestamp("s")),
            ]
        )

        issue_status_changelog_table = pa.Table.from_pylist(
            changes, schema=issue_status_changelog_schema
        )
        print(issue_status_changelog_table)
        yield issue_status_changelog_table

    def get_isis_project_names(self) -> list[str]:
        projects = self._client.get_all_projects()
        if not projects:
            return []

        return [
            project["name"]
            for project in projects
            if project["name"].startswith("[ISIS]")
        ]
