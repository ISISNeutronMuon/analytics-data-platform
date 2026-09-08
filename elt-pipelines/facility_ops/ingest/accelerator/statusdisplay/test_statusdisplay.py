from elt_common.testing.pipelines import AssertableCatalog

_namespace = "accelerator_statusdisplay"
_table_name = "elt_cycles"
_table_id = (_namespace, _table_name)


def test_expected_columns_created(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest()
    test_catalog.assert_has_columns(
        _table_id,
        [
            "id",
            "label",
            "status",
            "phases",
            "phases.element",  # phases is a list type
        ],
    )
    num_rows = test_catalog.get_num_rows(_table_id)
    assert num_rows >= 153, f"Found {num_rows} cycles, expected at least 153"

    test_catalog.clean_catalog()

    assert not test_catalog.catalog.table_exists(_table_id), (
        "Table should have been cleaned up"
    )


def test_multiple_runs(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest()
    n = test_catalog.get_num_rows(_table_id)
    run_test_ingest()
    run_test_ingest()

    # Check runs are overwriting, not appending
    test_catalog.assert_has_n_rows(_table_id, n)

    nss = test_catalog.catalog.list_namespaces()
    assert len(nss) == 1, "There should be a single namespace"
    tables = test_catalog.catalog.list_tables(_namespace)
    assert len(tables) == 1, "There should be a single table"
