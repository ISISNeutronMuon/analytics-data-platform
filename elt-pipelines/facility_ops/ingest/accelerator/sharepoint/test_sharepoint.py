from elt_common.testing.pipelines import AssertableCatalog

_namespace = "accelerator_sharepoint"
_equipment_id = (_namespace, "edr_equipment_mapping")
_downtime_id = (_namespace, "equipment_downtime_data_11_08_24")


def test_expected_columns_created(test_catalog: AssertableCatalog, run_test_ingest):
    run_test_ingest()
    test_catalog.assert_has_columns(
        _equipment_id,
        [
            "equipment_name",
            "equipment_category",
        ],
    )
    test_catalog.assert_at_least_rows(_equipment_id, 240)

    test_catalog.assert_has_columns(
        _downtime_id,
        [
            "Unnamed: 0.1",
            "Unnamed: 0",
            "ID",
            "Equipment",
            "FaultDate",
            "User Run",
            "Downtime (minutes)",
            "FaultTime",
            "Group",
            "DutyOfficer",
            "FaultDescription",
            "FaultRepair",
            "Managerscomments",
            "Manager email address",
            "DutyOfficer comments",
            "LogEntry",
            "downtimeh",
            "downtimem",
            "areacode",
            "subareacode",
            "subsubareacode",
            "Run time",
        ],
    )
    test_catalog.assert_at_least_rows(_downtime_id, 66000)
