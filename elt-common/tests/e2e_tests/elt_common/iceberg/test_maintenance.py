import dataclasses
from pyiceberg.catalog import Catalog as PyIcebergCatalog
import os
import pyarrow as pa

from elt_common.iceberg.trino import TrinoQueryEngine
from e2e_tests.conftest import Warehouse

TEST_NAMESPACE_PREFIX, TEST_TABLE_PREFIX, TEST_VIEW_PREFIX = "test_ns_", "test_table_", "test_view_"


@dataclasses.dataclass
class EltMaintenanceEnviron:
    warehouse: Warehouse

    def setup(self):
        # These values are set to default to work with the local docker-based in infra/local/docker-compose.yml
        env_vars = {
            "ELT_COMMON_ICEBERG_MAINT_TRINO_HTTP_SCHEME": "http",
            "ELT_COMMON_ICEBERG_MAINT_TRINO_HOST": "localhost",
            "ELT_COMMON_ICEBERG_MAINT_TRINO_PORT": "58088",
            "ELT_COMMON_ICEBERG_MAINT_TRINO_USER": "trino",
            "ELT_COMMON_ICEBERG_MAINT_TRINO_PASSWORD": "",
            "ELT_COMMON_ICEBERG_MAINT_TRINO_CATALOG": self.warehouse.name,
        }
        for key, value in env_vars.items():
            os.environ.setdefault(key, value)


# @pytest.fixture()
# def elt_maintenance_environ(warehouse: Warehouse):
#     env = EltMaintenanceEnviron(warehouse)
#     env.setup()
#     return env


def create_tables(
    warehouse: Warehouse,
    namespace_count: int = 1,
    table_count_per_ns: int = 1,
    snapshot_count: int = 1,
) -> PyIcebergCatalog:
    """Create tables and additional snapshots of tables for maintenance testing"""

    def append_snapshot(table_id: str, id_start: int):
        """Append new data to the table to create another Iceberg snapshot.
        If the table does not exist it is created."""
        rows_per_frame = 10
        id_end = id_start + rows_per_frame - 1
        test_data = pa.Table.from_pydict(
            {
                "id": pa.array([id for id in range(id_start, id_end + 1)]),
                "value": pa.array([f"value-{id}" for id in range(id_start, id_end + 1)]),
            }
        )
        if catalog.table_exists(table_id):
            table = catalog.load_table(table_id)
        else:
            table = catalog.create_table(table_id, schema=test_data.schema)

        table.append(test_data)
        return id_start + rows_per_frame

    catalog = warehouse.connect()

    for ns_index in range(namespace_count):
        ns_name = f"{TEST_NAMESPACE_PREFIX}{ns_index}"
        catalog.create_namespace(ns_name)
        for table_index in range(table_count_per_ns):
            id_start = 1
            for _ in range(snapshot_count):
                id_start = append_snapshot(f"{ns_name}.{TEST_TABLE_PREFIX}{table_index}", id_start)

    return catalog


def create_views(trino_query_engine, namespace: str, query_table: str):
    trino_query_engine.execute(
        f"create view {namespace}.{TEST_VIEW_PREFIX}0 as (select * from {namespace}.{query_table})"
    )


def test_trino_controller_list_tables_returns_only_iceberg_tables(
    warehouse, trino_engine: TrinoQueryEngine
):
    create_tables(warehouse, namespace_count=2, table_count_per_ns=2)
    create_views(trino_engine, TEST_NAMESPACE_PREFIX + "0", query_table=f"{TEST_TABLE_PREFIX}0")

    iceberg_tables = trino_engine.list_iceberg_tables()
    assert len(iceberg_tables) == 4
