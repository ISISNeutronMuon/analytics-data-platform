import copy
import os
from pathlib import Path
import tempfile
import subprocess as subp
import sys

from elt_common.testing.dlt import PyIcebergDestinationTestConfiguration
from fake_source import create_fake_source_db, update_fake_source_db
from fake_source.opralogmodel import ADDITIONAL_COLUMNS as OLOG_ADDITIONAL_COLS
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.table import Table as IcebergTable
import pytest

EL_SCRIPT = Path(__file__).parent.parent / "extract_and_load.py"


@pytest.fixture()
def fake_source_db():
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_url = f"sqlite:///{tmp_dir}/fake_source.sqlite"
        create_fake_source_db(db_url)
        yield db_url


def run_el_script(fake_source_db, destination_config, pipelines_dir):
    subp_env = copy.deepcopy(os.environ)
    destination_config.setup(subp_env)
    subp_env["OPRALOGWEB__SOURCES__CREDENTIALS"] = fake_source_db
    subp_env["DLT_DATA_DIR"] = pipelines_dir
    proc = subp.run(
        [sys.executable, EL_SCRIPT], env=subp_env, text=True, stderr=subp.STDOUT
    )

    if proc.returncode != 0:
        pytest.fail(reason=proc.stdout)


#######################################
# Validate destination tables
# Functions expect either 1 or 2 runs
# of the pipeline to have been performed
#######################################


def validate_chapter_entry_table(iceberg_table: IcebergTable, *, nruns: int):
    assert nruns in (1, 2)
    assert iceberg_table.scan().count() == 100 + (nruns - 1) * 10


def validate_entries_table(iceberg_table: IcebergTable, *, nruns: int):
    assert nruns in (1, 2)

    arrow_table: pa.Table = iceberg_table.scan().to_arrow()
    assert arrow_table.num_rows == 100 + (nruns - 1) * 10
    assert len(arrow_table["entry_id"].unique()) == arrow_table.num_rows

    # second run changed entry_id == 110
    entry_110 = arrow_table.filter((pc.field("entry_id") == 110)).to_pydict()
    if nruns == 1:
        assert entry_110["additional_comment"][0].strip() == "Comment 110"
    else:
        assert entry_110["additional_comment"][0].strip() == "Comment 110 updated."


def validate_more_entry_columns_table(iceberg_table: IcebergTable, *, nruns: int):
    assert nruns in (1, 2)

    arrow_table: pa.Table = iceberg_table.scan().to_arrow()
    assert arrow_table.num_rows == 400 + (nruns - 1) * 40

    # second run changed entry_id == 110
    entry_110 = arrow_table.filter(
        (
            (pc.field("entry_id") == 110)
            & (pc.field("additional_column_id") == OLOG_ADDITIONAL_COLS["Lost Time"])
        )
    ).to_pydict()
    if nruns == 1:
        assert entry_110["number_value"][0] == 5.1
    else:
        assert entry_110["number_value"][0] == 3.5


#######################################
# Tests
#######################################


def test_first_run_pulls_all_records(
    fake_source_db: str,
    destination_config: PyIcebergDestinationTestConfiguration,
    pipelines_dir: str,
):
    catalog = destination_config.warehouse.connect()
    expected_ns = "src_opralogweb"
    assert (expected_ns,) not in catalog.list_namespaces()

    run_el_script(fake_source_db, destination_config, pipelines_dir)

    assert (expected_ns,) in catalog.list_namespaces()
    # See fake_source/__init__.py for database contents
    expected_row_counts = {
        "chapter_entry": validate_chapter_entry_table,
        "logbook_chapter": 5,
        "logbooks": 1,
        "additional_columns": 4,
        "entries": validate_entries_table,
        "more_entry_columns": validate_more_entry_columns_table,
    }
    for name, validator in expected_row_counts.items():
        table = catalog.load_table((expected_ns, name))
        if isinstance(validator, int):
            assert table.scan().count() == validator
        else:
            validator(table, nruns=1)


def test_second_run_merges_updates(
    fake_source_db: str,
    destination_config: PyIcebergDestinationTestConfiguration,
    pipelines_dir: str,
):
    catalog = destination_config.warehouse.connect()
    expected_ns = "src_opralogweb"
    assert (expected_ns,) not in catalog.list_namespaces()

    run_el_script(fake_source_db, destination_config, pipelines_dir)
    update_fake_source_db(fake_source_db)
    run_el_script(fake_source_db, destination_config, pipelines_dir)

    assert (expected_ns,) in catalog.list_namespaces()
    expected_row_counts = {
        "chapter_entry": validate_chapter_entry_table,
        "logbook_chapter": 5,
        "logbooks": 1,
        "additional_columns": 4,
        "entries": validate_entries_table,
        "more_entry_columns": validate_more_entry_columns_table,
    }
    for name, validator in expected_row_counts.items():
        table = catalog.load_table((expected_ns, name))
        if isinstance(validator, int):
            assert table.scan().count() == validator
        else:
            validator(table, nruns=2)
