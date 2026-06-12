from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.lib
import pytest
import sqlalchemy as sa

from elt_common.extract import ResourceWriteProperties, Watermark
from elt_common.sources.sqldatabase import SqlDatabaseExtract, SqlDatabaseSourceConfig, TableInfo

person_values = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
    {"name": "Carol", "age": 40},
    {"name": "Dave", "age": 20},
    {"name": "Eve", "age": 35},
]

pet_values = [
    {"owner_id": 1, "name": "Rex"},
    {"owner_id": 1, "name": "Fluffy"},
    {"owner_id": 3, "name": "Buddy"},
]


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
            person_values,
        )
        conn.execute(
            pets.insert(),
            pet_values,
        )

    return engine


def _create_config(db_path: Path, chunk_size: int = 5000):
    return SqlDatabaseSourceConfig(
        drivername="sqlite", database=str(db_path), chunk_size=chunk_size, username=None
    )


def _table_rows(table: pa.Table) -> list[dict]:
    return [dict(row) for row in table.to_pylist()]


def test_sql_database_no_table_info_nothing_returned(tmp_path: Path):
    db_path = tmp_path / "test.db"
    _create_sqlite_database(db_path)
    source_config = _create_config(db_path)

    class Extract(SqlDatabaseExtract):
        def table_info(self) -> dict[str, Optional[TableInfo]]:
            return {}

    e = Extract(source_config)
    assert len(list(e.extract_resource_properties())) == 0


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 4, 5, 6, 1000])
def test_sql_database_reads_table_in_chunks(tmp_path: Path, chunk_size):
    db_path = tmp_path / "test.db"
    _create_sqlite_database(db_path)
    source_config = _create_config(db_path, chunk_size=chunk_size)

    class Extract(SqlDatabaseExtract):
        def table_info(self) -> dict[str, Optional[TableInfo]]:
            return {"people": None}

    e = Extract(source_config)

    expected_last_chunk_size = (
        len(person_values) % chunk_size if len(person_values) % chunk_size > 0 else chunk_size
    )

    for table_name, props in e.extract_resource_properties():
        data = props.extractor(None)
        chunks = list(data)
        for i, chunk in enumerate(chunks):
            expected_length = chunk_size if i < len(chunks) - 1 else expected_last_chunk_size
            assert len(chunk) == expected_length


def test_sql_database_reads_multiple_tables(tmp_path: Path):
    db_path = tmp_path / "test.db"
    _create_sqlite_database(db_path)
    source_config = _create_config(db_path)

    class Extract(SqlDatabaseExtract):
        def table_info(self) -> dict[str, Optional[TableInfo]]:
            return {"people": None, "pets": None}

    e = Extract(source_config)

    result = {}
    for table_name, props in e.extract_resource_properties():
        data = pyarrow.lib.concat_tables(table for table in props.extractor(None))
        result[table_name] = data

    assert len(result) == 2

    people = _table_rows(result["people"])
    pets = _table_rows(result["pets"])
    assert len(people) == len(person_values)
    assert len(pets) == len(pet_values)

    people_without_ids = [{k: v for k, v in person.items() if k != "id"} for person in people]
    assert people_without_ids == person_values

    pets_without_ids = [{k: v for k, v in pet.items() if k != "id"} for pet in pets]
    assert pets_without_ids == pet_values


def test_sql_database_write_properties_returned(tmp_path: Path):
    db_path = tmp_path / "test.db"
    _create_sqlite_database(db_path)
    source_config = _create_config(db_path)

    people_write_properties = ResourceWriteProperties()
    pet_write_properties = ResourceWriteProperties(write_mode="replace")

    class Extract(SqlDatabaseExtract):
        def table_info(self) -> dict[str, Optional[TableInfo]]:
            return {
                "people": TableInfo(write_properties=people_write_properties),
                "pets": TableInfo(write_properties=pet_write_properties),
            }

    e = Extract(source_config)
    for table_name, props in e.extract_resource_properties():
        if table_name == "people":
            assert props.write_properties == people_write_properties
        else:
            assert props.write_properties == pet_write_properties


def test_sql_database_watermarks_filter_results(tmp_path: Path):
    db_path = tmp_path / "test.db"
    _create_sqlite_database(db_path)
    source_config = _create_config(db_path)

    class Extract(SqlDatabaseExtract):
        def table_info(self) -> dict[str, Optional[TableInfo]]:
            return {"people": None}

    e = Extract(source_config)

    for table_name, props in e.extract_resource_properties():
        id_watermark = 2
        data = pyarrow.lib.concat_tables(
            table for table in props.extractor(Watermark("id", id_watermark))
        )

        assert len(data) == len(person_values) - id_watermark
        assert min(data["id"].to_pylist()) > id_watermark

    for table_name, props in e.extract_resource_properties():
        tables = [table for table in props.extractor(Watermark("age", 39))]
        data = pyarrow.lib.concat_tables(tables)
        assert len(data) == 1, "Only the one person with age > 39 should be returned"
