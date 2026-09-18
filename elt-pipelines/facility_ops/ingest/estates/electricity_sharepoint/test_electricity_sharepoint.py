import datetime
import json

from elt_common.testing.pipelines import AssertableCatalog

_namespace = "estates_electricity_sharepoint"
_table_id = (_namespace, "rdm_data")


def test_expected_columns_created(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest()
    test_catalog.assert_has_columns(
        _table_id,
        [
            "date_time",
            "isis_elec_total_power_mw",
            "file_name",
        ],
    )
    test_catalog.assert_at_least_rows(_table_id, 100)

    arrow_table = test_catalog.catalog.load_table(_table_id).scan().to_arrow()

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    for d in arrow_table["date_time"].to_pylist():
        diff = now - d
        assert diff.total_seconds() > 0 and diff.days <= 1, (
            "Non-backfill electricity_sharepoint values should be from the last 48 hours"
        )


def test_backfill_globs_xlsx(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest(
        backfill="True", backfill_globs=json.dumps(["**/JAN2025 ISIS.xlsx"])
    )
    # Historical data shouldn't change so we should get exactly the correct number of rows
    test_catalog.assert_has_n_rows(_table_id, 34719)


def test_backfill_globs_daily_csv(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest(backfill="True", backfill_globs=json.dumps(["**/250708-daily.csv"]))
    # Historical data shouldn't change so we should get exactly the correct number of rows
    test_catalog.assert_has_n_rows(_table_id, 764)
