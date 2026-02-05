import re


from elt_common.iceberg.trino import TrinoQueryEngine
from pytest_mock import MockerFixture


def test_trino_query_engine_list_tables_returns_only_iceberg_tables(
    mocker: MockerFixture,
):
    mocker.patch.object(TrinoQueryEngine, "_create_engine", mocker.MagicMock())
    trino = TrinoQueryEngine(
        host="localhost", port="8088", catalog="default", user="test_user", password="default"
    )
    trino_execute_spy = mocker.spy(trino, "execute")

    trino.list_iceberg_tables()

    assert trino_execute_spy.call_count == 1
    expected_cmd_re = re.compile(r"^select \* from system.iceberg_tables$")
    command_match = expected_cmd_re.match(trino_execute_spy.call_args[0][0])
    assert command_match is not None
