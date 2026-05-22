from pathlib import Path

import pyarrow as pa
import sqlalchemy as sa

from elt_common.sources.sqldatabase import SqlDatabaseSourceConfig, sql_database


def _create_sqlite_database(db_path: Path) -> sa.engine.Engine:
    metadata = sa.MetaData()
    people = sa.Table(
        "people",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        sa.Column("age", sa.Integer),
    )
    pets = sa.Table(
        "pets",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("owner_id", sa.Integer),
        sa.Column("name", sa.String),
    )

    engine = sa.create_engine(f"sqlite:///{db_path}")
    metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(
            people.insert(),
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Carol", "age": 40},
                {"name": "Dave", "age": 20},
                {"name": "Eve", "age": 35},
            ],
        )
        conn.execute(
            pets.insert(),
            [
                {"owner_id": 1, "name": "Rex"},
                {"owner_id": 1, "name": "Fluffy"},
                {"owner_id": 3, "name": "Buddy"},
            ],
        )

    return engine


def _table_rows(table: pa.Table) -> list[dict]:
    return [dict(row) for row in table.to_pylist()]


def test_sql_database_reads_table_in_chunks(tmp_path: Path):
    db_path = tmp_path / "test.db"
    _create_sqlite_database(db_path)

    source_config = SqlDatabaseSourceConfig(
        drivername="sqlite",
        database=str(db_path),
        chunk_size=2,
    )

    partitions = list(sql_database(source_config, ["people"]))

    assert all(isinstance(partition, pa.Table) for partition in partitions)
    assert [partition.num_rows for partition in partitions] == [2, 2, 1]

    rows = [row for partition in partitions for row in _table_rows(partition)]
    assert rows == [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Carol", "age": 40},
        {"id": 4, "name": "Dave", "age": 20},
        {"id": 5, "name": "Eve", "age": 35},
    ]


def test_sql_database_reads_multiple_tables(tmp_path: Path):
    db_path = tmp_path / "test.db"
    _create_sqlite_database(db_path)

    source_config = SqlDatabaseSourceConfig(
        drivername="sqlite",
        database=str(db_path),
        chunk_size=100,
    )

    partitions = list(sql_database(source_config, ["people", "pets"]))

    assert len(partitions) == 2
    assert partitions[0].num_rows == 5
    assert partitions[1].num_rows == 3

    people_rows = _table_rows(partitions[0])
    pets_rows = _table_rows(partitions[1])

    assert people_rows[0]["name"] == "Alice"
    assert pets_rows[0] == {"id": 1, "owner_id": 1, "name": "Rex"}
    assert pets_rows == [
        {"id": 1, "owner_id": 1, "name": "Rex"},
        {"id": 2, "owner_id": 1, "name": "Fluffy"},
        {"id": 3, "owner_id": 3, "name": "Buddy"},
    ]
