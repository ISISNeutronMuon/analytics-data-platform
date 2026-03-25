from pathlib import Path
import tempfile
from typing import Any

from opralogweb import Extract
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import sqlalchemy as sa

# This directory contains sql scripts to setup a minimal fake database to
# test the extraction logic
TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture()
def fake_db():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_file = Path(tmp_dir) / "fake_db.db"
        _execute_sql_scripts(TEST_DATA_DIR / "base", db_file)
        yield db_file


#######################################
# Tests
#######################################
def test_first_run_pulls_all_records(fake_db: Path):
    extractor = _create_extractor(fake_db)
    data_items = list(extractor.extract(cursor_info={}))

    assert len(data_items) == 6  # number of tables
    # See fake_source/__init__.py for database contents
    expected_row_counts = {
        "ChapterEntry": 5,
        "LogbookChapter": 2,
        "Logbooks": 1,
        "AdditionalColumns": 4,
        "Entries": _validate_entries_table,
        "MoreEntryColumns": _validate_more_entry_columns_table,
    }
    for table_name, arrow_table in data_items:
        validator = expected_row_counts[table_name]
        if isinstance(validator, int):
            assert arrow_table.num_rows == validator
        else:
            validator(arrow_table, first_run=True)


def test_second_run_merges_updates(fake_db: Path):
    extractor = _create_extractor(fake_db)

    # run once
    _ = list(extractor.extract(cursor_info={}))
    max_values = _get_table_max_values(extractor._engine, extractor._metadata)

    # do an update
    _execute_sql_scripts(TEST_DATA_DIR / "update_1", Path(fake_db))

    # extract
    tables_info = extractor.tables()
    cursor_info = {}
    for table_name, info in tables_info.items():
        if info.cursor_column is not None:
            cursor_info[table_name] = {
                "column": info.cursor_column,
                "max_value": max_values[table_name][info.cursor_column],
            }

    data_items_second_run = list(extractor.extract(cursor_info=cursor_info))
    expected_row_counts = {
        "ChapterEntry": 2,
        "LogbookChapter": 5,
        "Logbooks": 1,
        "AdditionalColumns": 4,
        "Entries": _validate_entries_table,
        "MoreEntryColumns": _validate_more_entry_columns_table,
    }
    for table_name, arrow_table in data_items_second_run:
        validator = expected_row_counts[table_name]
        if isinstance(validator, int):
            assert arrow_table.num_rows == validator
        else:
            validator(arrow_table, first_run=False)


#######################################
# Helpers
#######################################


def _create_extractor(db_file: Path):
    return Extract(
        Extract.source_config_cls(drivername="sqlite", database=str(db_file))
    )


def _execute_sql_scripts(scripts_dir: Path, db_file: Path):
    engine = sa.create_engine(f"sqlite:///{db_file}", echo=False)
    with engine.connect() as conn:
        raw_conn = conn.connection
        for sql_file in scripts_dir.glob("*.sql"):
            raw_conn.executescript(sql_file.read_text())


def _update_fake_db(db_file: Path):
    _execute_sql_scripts(TEST_DATA_DIR / "update_1", db_file)


def _get_table_max_values(
    engine: sa.Engine, meta: sa.MetaData
) -> dict[str, dict[str, Any]]:
    """Return the maximum value of every column in every table."""
    result: dict[str, dict[str, Any]] = {}
    with engine.connect() as conn:
        for table in meta.sorted_tables:
            result[table.name] = {
                col.name: conn.execute(sa.select(sa.func.max(col))).scalar_one_or_none()
                for col in table.columns
            }
    return result


# See sql scripts in data/ for where thes values originate
def _validate_entries_table(arrow_table: pa.Table, *, first_run: bool):
    assert arrow_table.num_rows == (5 if first_run else 3)
    assert len(arrow_table["EntryId"].unique()) == arrow_table.num_rows

    # second run changed EntryId == 104
    entry_104 = arrow_table.filter((pc.field("EntryId") == 104)).to_pydict()
    if first_run:
        assert entry_104["AdditionalComment"][0].strip() == "Comment 104."
    else:
        assert entry_104["AdditionalComment"][0].strip() == "Comment 104 updated."


def _validate_more_entry_columns_table(arrow_table: pa.Table, *, first_run: bool):
    assert arrow_table.num_rows == (20 if first_run else 12)

    # second run changed entry_id == 104.
    # check the lost time changed field (last record for each entryid, see MoreEntryColumns)
    entry_104 = arrow_table.filter((pc.field("EntryId") == 104)).to_pydict()
    if first_run:
        assert entry_104["NumberValue"][-1] == 4.5
    else:
        assert entry_104["NumberValue"][-1] == 5.1
