from elt_common.testing.pipelines import AssertableCatalog

_namespace = "computing_jira"
_issues_table_id = (_namespace, "isis_jira_issues")
_status_changelogs_table_id = (_namespace, "issue_status_changelogs")


def test_expected_columns_created(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest()
    test_catalog.assert_has_columns(
        _issues_table_id,
        [
            "issue_key",
            "issue_type",
            "project_name",
            "status",
            "priority",
            "created",
            "updated",
            "teams",
            "teams.element",
        ],
    )
    test_catalog.assert_at_least_rows(_issues_table_id, 500)

    test_catalog.assert_has_columns(
        _status_changelogs_table_id,
        [
            "issue_key",
            "from_status",
            "to_status",
            "changed_at",
        ],
    )
    test_catalog.assert_at_least_rows(_status_changelogs_table_id, 1200)
