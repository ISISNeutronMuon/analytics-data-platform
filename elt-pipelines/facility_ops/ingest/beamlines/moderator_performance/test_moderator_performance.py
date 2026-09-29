import datetime
import json

from elt_common.testing.pipelines import AssertableCatalog

_archive_mount = "//isis/inst$"
_namespace = "beamlines_moderator_performance"
_table_id = (_namespace, "monitor_peaks")


def test_cycle_22_5(test_catalog: AssertableCatalog, run_test_ingest):
    runs_config = {"PEARL": {"cycles": ["22_5"]}}
    run_test_ingest(archive_mount=_archive_mount, runs_config=json.dumps(runs_config))
    # There are 24 nxs files in this cycle:
    # - 6 are skipped for having proton_charge_uamps < 1.0
    # - 1 fails to fit because 'Residuals are not finite in the initial point'
    # - 17 are expected to be ingested
    test_catalog.assert_has_n_rows(_table_id, 17)


def test_specific_runs(test_catalog: AssertableCatalog, run_test_ingest):
    runs_config = {"PEARL": {"cycles": ["20_2", "24_1"], "runs": [113285, 119342]}}
    run_test_ingest(
        archive_mount=_archive_mount,
        run_mode="backfill",
        runs_config=json.dumps(runs_config),
    )
    test_catalog.assert_has_n_rows(_table_id, 2)
    table = test_catalog.catalog.load_table(_table_id)
    as_dicts = table.scan().to_arrow().to_pylist()
    run_113285 = [row for row in as_dicts if row["run_number"] == 113285][0]
    assert run_113285 == {
        "beamline": "PEARL",
        "run_number": 113285,
        "cycle_name": "cycle_20_2",
        "run_start": datetime.datetime(2020, 9, 4, 11, 25, 33),
        "proton_charge": 150.05267333984375,
        "peak_centre": 4986.055576195616,
        "peak_centre_error": 1.6892716476407574,
        "peak_amplitude": 19.010408807094493,
        "peak_amplitude_error": 0.01366393912502226,
        "peak_sigma": 1440.2507456007272,
        "peak_sigma_error": 2.567536091658019,
    }

    run_119342 = [row for row in as_dicts if row["run_number"] == 119342][0]
    assert run_119342 == {
        "beamline": "PEARL",
        "run_number": 119342,
        "cycle_name": "cycle_24_1",
        "run_start": datetime.datetime(2024, 4, 19, 16, 35, 51),
        "proton_charge": 150.05946350097656,
        "peak_centre": 4869.83600777918,
        "peak_centre_error": 2.11100758876813,
        "peak_amplitude": 19.00300934747022,
        "peak_amplitude_error": 0.013296865545751246,
        "peak_sigma": 1522.891001186718,
        "peak_sigma_error": 3.0031811376728643,
    }


def test_default_run_mode_only_most_recent_cycle(
    test_catalog: AssertableCatalog, run_test_ingest
):
    runs_config = {"PEARL": {"cycles": ["20_2", "24_1"], "runs": [113285, 119342]}}
    run_test_ingest(archive_mount=_archive_mount, runs_config=json.dumps(runs_config))
    test_catalog.assert_has_n_rows(_table_id, 1)


def test_empty_cycles_nothing_ingested(
    test_catalog: AssertableCatalog, run_test_ingest
):
    runs_config = {"PEARL": {"cycles": []}}
    run_test_ingest(archive_mount=_archive_mount, runs_config=json.dumps(runs_config))
    test_catalog.assert_has_exact_tables(_namespace, [])
