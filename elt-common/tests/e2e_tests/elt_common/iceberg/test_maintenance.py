import pytest

from elt_common.iceberg.trino import TrinoQueryEngine
from elt_common.iceberg.maintenance import IcebergTableMaintenaceSql
from e2e_tests.conftest import Warehouse

TEST_VIEW_PREFIX = "test_view_"


def create_views(trino_query_engine, namespace: str, query_table: str):
    trino_query_engine.execute(
        f"create view {namespace}.{TEST_VIEW_PREFIX}0 as (select * from {namespace}.{query_table})"
    )


def test_trino_query_engine_list_tables_returns_only_iceberg_tables(
    warehouse: Warehouse, trino_engine: TrinoQueryEngine
):
    with warehouse.create_test_tables(
        namespace_count=2,
        table_count_per_ns=2,
    ):
        create_views(trino_engine, "test_ns_0", query_table="test_table_0")

        iceberg_tables = trino_engine.list_iceberg_tables()
        assert len(iceberg_tables) == 4


def test_iceberg_maintenance_expire_snapshots_raises_error_if_retention_too_low(
    warehouse: Warehouse, trino_engine: TrinoQueryEngine
):
    with warehouse.create_test_tables(namespace_count=2, table_count_per_ns=2, snapshot_count=10):
        iceberg_maint = IcebergTableMaintenaceSql(trino_engine)
        table_id = trino_engine.list_iceberg_tables()[0]

        with pytest.raises(ValueError):
            iceberg_maint.expire_snapshots(table_id, retention_threshold="0d")


def test_iceberg_maintenance_expire_snapshots_with_session_retention(
    warehouse: Warehouse, trino_engine: TrinoQueryEngine
):
    snapshot_count = 10
    with warehouse.create_test_tables(
        namespace_count=1, table_count_per_ns=1, snapshot_count=snapshot_count
    ):
        trino_engine.execute(f"set session {warehouse.name}.expire_snapshots_min_retention = '0d'")
        iceberg_maint = IcebergTableMaintenaceSql(trino_engine)
        table_id = trino_engine.list_iceberg_tables()[0]

        iceberg_maint.expire_snapshots(table_id, retention_threshold="0d")

        catalog = warehouse.connect()
        table = catalog.load_table(table_id)
        assert len(table.snapshots()) < snapshot_count
