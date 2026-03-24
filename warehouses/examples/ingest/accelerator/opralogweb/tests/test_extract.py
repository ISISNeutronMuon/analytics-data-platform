import tempfile

# from elt_common.typing import CursorInfo
from fake_source import (
    create_fake_source_db,
    get_table_max_values,
    update_fake_source_db,
)
from fake_source.opralogmodel import ADDITIONAL_COLUMNS as OLOG_ADDITIONAL_COLS
from opralogweb import Extract
import pyarrow as pa
import pyarrow.compute as pc
import pytest

#######################################
# Helpers
#######################################


@pytest.fixture()
def fake_db():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_file = f"///{tmp_dir}/fake_db.sqlite"
        create_fake_source_db(db_url=f"sqlite:{db_file}")
        yield db_file


def create_extractor(db_file: str):
    return Extract(Extract.source_config_cls(drivername="sqlite", database=db_file))


def validate_chapter_entry_table(arrow_table: pa.Table, *, nruns: int):
    assert nruns in (1, 2)
    assert arrow_table.num_rows == 100 if nruns == 2 else 10


def validate_entries_table(arrow_table: pa.Table, *, nruns: int):
    assert nruns in (1, 2)

    assert arrow_table.num_rows == 100 if nruns == 2 else 10
    assert len(arrow_table["EntryId"].unique()) == arrow_table.num_rows

    # second run changed EntryId == 110
    entry_110 = arrow_table.filter((pc.field("EntryId") == 110)).to_pydict()
    if nruns == 1:
        assert entry_110["AdditionalComment"][0].strip() == "Comment 110"
    else:
        assert entry_110["AdditionalComment"][0].strip() == "Comment 110 updated."


def validate_more_entry_columns_table(arrow_table: pa.Table, *, nruns: int):
    assert nruns in (1, 2)

    assert arrow_table.num_rows == 400 + (nruns - 1) * 40

    # second run changed entry_id == 110
    entry_110 = arrow_table.filter(
        (
            (pc.field("EntryId") == 110)
            & (pc.field("AdditionalColumnId") == OLOG_ADDITIONAL_COLS["Lost Time"])
        )
    ).to_pydict()
    if nruns == 1:
        assert entry_110["NumberValue"][0] == 5.1
    else:
        assert entry_110["NumberValue"][0] == 3.5


#######################################
# Tests
#######################################
def test_first_run_pulls_all_records(fake_db: str):
    extractor = create_extractor(fake_db)
    data_items = list(extractor.extract(cursor_info={}))

    assert len(data_items) == 6
    # See fake_source/__init__.py for database contents
    expected_row_counts = {
        "ChapterEntry": validate_chapter_entry_table,
        "LogbookChapter": 5,
        "Logbooks": 1,
        "AdditionalColumns": 4,
        "Entries": validate_entries_table,
        "MoreEntryColumns": validate_more_entry_columns_table,
    }
    for table_name, arrow_table in data_items:
        validator = expected_row_counts[table_name]
        if isinstance(validator, int):
            assert arrow_table.num_rows == validator
        else:
            validator(arrow_table, nruns=1)


def test_second_run_merges_updates(fake_db: str):
    extractor = create_extractor(fake_db)

    # first run
    _ = list(extractor.extract(cursor_info={}))
    # do an update
    db_url = f"sqlite:{fake_db}"
    max_values = get_table_max_values(db_url)
    update_fake_source_db(db_url)

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
        "ChapterEntry": validate_chapter_entry_table,
        "LogbookChapter": 5,
        "Logbooks": 1,
        "AdditionalColumns": 4,
        "Entries": validate_entries_table,
        "MoreEntryColumns": validate_more_entry_columns_table,
    }
    for table_name, arrow_table in data_items_second_run:
        validator = expected_row_counts[table_name]
        if isinstance(validator, int):
            assert arrow_table.num_rows == validator
        else:
            validator(arrow_table, nruns=2)
