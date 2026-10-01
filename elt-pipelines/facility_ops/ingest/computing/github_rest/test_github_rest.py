from elt_common.testing.pipelines import AssertableCatalog


_namespace = "computing_github_rest"
_repositories_table_id = (_namespace, "repositories")


def test_expected_columns_created(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest()
    test_catalog.assert_has_columns(
        _repositories_table_id,
        [
            "name",
            "owner",
            "public",
            "fork",
            "default_branch",
            "repo_size_kilobytes",
            "license",
            "readme_size_kilobytes",
        ],
    )
    test_catalog.assert_at_least_rows(_repositories_table_id, 1)
