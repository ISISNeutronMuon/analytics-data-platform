from typing import Dict
import re


from click.testing import CliRunner
from elt_common.iceberg.trino import TrinoCredentials, TrinoQueryEngine
from elt_common.iceberg.maintenance import cli, IcebergTableMaintenaceSql
from e2e_tests.conftest import Warehouse
import pytest
from pytest_mock import MockerFixture

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


@pytest.mark.parametrize(
    "command,command_args",
    [
        ("expire_snapshots", {"retention_threshold": "0d"}),
        ("optimize", {}),
        ("optimize_manifests", {}),
        ("remove_orphan_files", {"retention_threshold": "0d"}),
    ],
)
def test_iceberg_maintenance_commands_run_expected_trino_alter_table_command(
    warehouse: Warehouse,
    trino_engine: TrinoQueryEngine,
    mocker: MockerFixture,
    command: str,
    command_args: Dict[str, str],
):
    with warehouse.create_test_tables(namespace_count=1, table_count_per_ns=1, snapshot_count=10):
        iceberg_maint = IcebergTableMaintenaceSql(trino_engine)
        table_id = trino_engine.list_iceberg_tables()[0]

        trino_execute_spy = mocker.spy(trino_engine, "execute")
        getattr(iceberg_maint, command)(table_id, **command_args)

        assert trino_execute_spy.call_count == 1
        expected_cmd_re = re.compile(
            rf"^alter table test_ns_0.test_table_0 execute ({command})(.+)?$"
        )
        command_match = expected_cmd_re.match(trino_execute_spy.call_args[0][0])
        assert command_match is not None
        assert command_match.group(1) == command
        if command_args:
            for key in command_args.keys():
                assert key in command_match.group(2)


def test_iceberg_maintenance_cli(mocker: MockerFixture):
    mock_from_env = mocker.patch.object(TrinoCredentials, "from_env", spec=TrinoCredentials)
    mock_from_env.return_value = TrinoCredentials("host", "1234", "catalog", "user", "password")
    mock_trino_list_tables = mocker.patch.object(
        TrinoQueryEngine, "list_iceberg_tables", spec=TrinoQueryEngine
    )
    mock_trino_list_tables.return_value = ["ns1.table1", "ns2.table2"]
    mock_trino_execute = mocker.patch.object(TrinoQueryEngine, "execute", spec=TrinoQueryEngine)

    runner = CliRunner()
    result = runner.invoke(cli)

    assert result.stderr == ""
    assert result.exit_code == 0

    mock_from_env.assert_called_once()
    # 4 calls per table, 1 per routine
    assert mock_trino_execute.call_count == 8
