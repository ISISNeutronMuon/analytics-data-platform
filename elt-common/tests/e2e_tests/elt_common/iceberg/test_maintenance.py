import dataclasses
from pyiceberg.catalog import Catalog as PyIcebergCatalog
import os
import pyarrow as pa

from e2e_tests.conftest import Warehouse


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


def populate_warehouse(warehouse: Warehouse) -> PyIcebergCatalog:
    """Create tables and additional snapshots of tables for maintenance testing"""

    def append_snapshot(table, id_start):
        """Append new data to the table to create another Iceberg snapshot"""
        rows_per_frame = 10
        test_data = pa.Table.from_pydict(
            {
                "id": pa.array([id for id in range(id_start, rows_per_frame + 1)]),
                "value": pa.array([f"value-{id}" for id in range(id_start, rows_per_frame + 1)]),
            }
        )

        # NOTE: ADD RETRY as appending too fast can cause race conditions
        table.append(test_data)
        return id_start + rows_per_frame

    catalog = warehouse.connect()
    namespace_prefix, table_prefix = "test_ns_", "test_table_"
    namespaces_count, tables_count = 1, 1
    snapshot_count = 1

    for ns_index in range(namespaces_count):
        ns_name = f"{namespace_prefix}{ns_index}"
        catalog.create_namespace(ns_name)
        for table_index in range(tables_count):
            table = catalog.create_table(
                f"{ns_name}.{table_prefix}{table_index}",
                schema=pa.schema([("id", pa.int64()), ("value", pa.string())]),
            )
            id_start = 1
            for _ in range(snapshot_count):
                id_start = append_snapshot(table, id_start)

    return catalog


def test_trino_controller_list_tables_returns_only_iceberg_tables(warehouse, trino_catalog):
    populate_warehouse(warehouse)

    # query_engine = TrinoQueryEngine(TrinoCredentials.from_env())
    # tables = query_engine.list_iceberg_tables_for_maintenance()

    # assert len(tables) > 0
