import enum

from atlassian import Jira
from elt_common.extract import BaseExtract, ResourceProperties
from pydantic_settings import BaseSettings

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


class AtlassianCredentials(BaseSettings):
    jira_url: str
    email_address: str
    api_token: str
    cloud: bool = True


class Extract(BaseExtract[AtlassianCredentials]):
    config_cls = AtlassianCredentials

    def __init__(self, cfg: AtlassianCredentials):
        super().__init__(cfg)
        self._client = Jira(
            cfg.jira_url, cfg.email_address, cfg.api_token, cloud=cfg.cloud
        )

    def extract_resource_properties(self):
        yield (
            ("all_jira_issues", ResourceProperties(extractor=self.extract_jira_issues)),
        )

    def extract_jira_issues(self) -> pa.Table:
        project_names = self.extract_isis_project_names()

        issues = []
        for project_name in project_names:
            project_issues = self._client.get_all_project_issues(
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
                team_names = (
                    [team["value"] for team in teams] if teams is not None else None
                )

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
                pa.field("created", pa.date32()),
                pa.field("updated", pa.date32()),
                pa.field("teams", pa.list_(pa.string())),
            ]
        )

        issues_table = pa.Table.from_pylist(issues, schema=issues_schema)
        print(issues_table)
        return issues_table

    def extract_isis_project_names(self) -> list[str]:
        projects = self._client.get_all_projects()
        return [
            project["name"]
            for project in projects
            if project["name"].startswith("[ISIS]")
        ]


if __name__ == "__main__":
    extract = Extract(AtlassianCredentials())
    extract.extract_jira_issues()
