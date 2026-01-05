import copy
import os
from pathlib import Path
import tempfile
import shutil
import subprocess as subp
import sys

from elt_common.testing.dlt import PyIcebergDestinationTestConfiguration
from elt_common.testing.lakekeeper import Warehouse
from fake_source import create_fake_source_db, update_fake_source_db

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
    expected_row_counts = {
        "chapter_entry": 100,
        "logbook_chapter": 5,
        "logbooks": 1,
        "additional_columns": 4,
        "entries": 100,
        "more_entry_columns": 400,
    }
    for name, expected_count in expected_row_counts.items():
        table = catalog.load_table((expected_ns, name))
        assert table.scan().count() == expected_count


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
        "chapter_entry": 110,
        "logbook_chapter": 5,
        "logbooks": 1,
        "additional_columns": 4,
        "entries": 110,
        "more_entry_columns": 440,
    }
    for name, expected_count in expected_row_counts.items():
        table = catalog.load_table((expected_ns, name))
        assert table.scan().count() == expected_count
