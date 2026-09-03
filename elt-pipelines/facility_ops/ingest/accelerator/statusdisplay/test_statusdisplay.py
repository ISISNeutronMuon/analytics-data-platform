from pyiceberg.catalog import Catalog

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
    test_catalog.assert_has_n_rows(_table_id, 153)

    test_catalog.clean_catalog()

    def check_table_cleaned(catalog):
        assert not catalog.table_exists(_table_id), "Table should have been cleaned up"

    test_catalog.do_something(check_table_cleaned)


def test_multiple_runs(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest()
    n = test_catalog.get_num_rows(_table_id)
    run_test_ingest()
    run_test_ingest()

    # Check runs are overwriting, not appending
    test_catalog.assert_has_n_rows(_table_id, n)

    def check_single_ns_and_table(catalog: Catalog):
        nss = catalog.list_namespaces()
        assert len(nss) == 1, "There should be a single namespace"
        catalog.namespace_exists(_namespace)
        assert len(catalog.list_tables(_namespace)) == 1, (
            "There should be a single table"
        )

    test_catalog.do_something(check_single_ns_and_table)
