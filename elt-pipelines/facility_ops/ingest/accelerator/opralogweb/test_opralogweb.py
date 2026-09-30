from elt_common.testing.pipelines import AssertableCatalog

_namespace = "accelerator_opralogweb"
_expected_tables = [
    "ChapterEntry",
    "LogbookChapter",
    "Logbooks",
    "AdditionalColumns",
    "Entries",
]


def test_expected_tables_read(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest(row_limit="5")
    for t in _expected_tables:
        test_catalog.assert_has_n_rows((_namespace, t), 5)
    test_catalog.assert_at_least_rows((_namespace, "MoreEntryColumns"), 1)
