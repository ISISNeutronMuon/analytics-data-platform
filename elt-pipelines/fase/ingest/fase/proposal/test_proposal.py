import json
import pytest

from elt_common.testing.pipelines import AssertableCatalog

_ns = "fase_proposal"


def test_expected_columns_created(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest(tables=json.dumps(["call"]))
    expected_columns = [
        "call_id",
        "call_short_code",
        "start_call",
        "end_call",
        "start_review",
        "end_review",
    ]
    test_catalog.assert_has_columns((_ns, "call"), expected_columns)
    test_catalog.clean_catalog()


def test_multiple_tables(test_catalog: AssertableCatalog, run_test_ingest):
    tables = ["call", "countries", "questions"]
    run_test_ingest(tables=json.dumps(tables), row_limit="5")
    test_catalog.assert_has_exact_tables(_ns, tables)
    test_catalog.clean_catalog()


@pytest.mark.parametrize("row_limit", [1, 5, 20])
def test_row_limit_applied(test_catalog: AssertableCatalog, run_test_ingest, row_limit):
    run_test_ingest(row_limit=str(row_limit), tables=json.dumps(["call"]))
    test_catalog.assert_has_n_rows((_ns, "call"), row_limit)
    test_catalog.clean_catalog()
