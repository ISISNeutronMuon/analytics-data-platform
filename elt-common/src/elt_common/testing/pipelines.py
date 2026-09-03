"""Pytest fixtures and utilities for e2e testing ingest pipelines.

The `test_catalog` fixture creates a temporary file based iceberg warehouse,
loads its catalog, and provides methods which tests can use to make assertions
about the data which gets written to it.

`run_test_ingest` exposes a method that runs the pipeline under test, optionally
with configuration values provided as kwargs.

Both rely on the test file being named 'test_<job>.py' and existing in the same
directory as the pipeline it tests, in the elt-common directory structure:

<warehouse_name>/
|-- ingest/
|   |-- <domain>/
|   |   |-- <job>/
|   |   |   |-- <job>.py
|   |   |   |-- test_<job>.py
"""

import logging
from collections.abc import Callable
from pathlib import Path

import pytest
from pyiceberg.catalog import Catalog

from elt_common.ingest import run_ingest
from elt_common.pipeline import create_ingest_manifest
from elt_common.testing.sqlcatalog import SqlCatalogWarehouse

LOGGER = logging.getLogger(__name__)


class AssertableCatalog:
    """Wraps an iceberg catalog with convenience methods for making assertions
    about the data in it"""

    def __init__(self, catalog: Catalog):
        self._catalog = catalog

    def clean_catalog(self):
        for ns in self._catalog.list_namespaces():
            tables = self._catalog.list_tables(ns)
            for qualified_table_name in tables:
                self._catalog.purge_table(qualified_table_name)

            self._catalog.drop_namespace(ns)

    def do_something(self, something: Callable[[Catalog], None]):
        something(self._catalog)

    def assert_has_exact_tables(self, namespace: str, tables: list[str]):
        actual = self._catalog.list_tables(namespace)
        assert set(actual) == {(namespace, table) for table in tables}

    def assert_has_columns(self, table_id: tuple[str, ...], column_names):
        assert self._catalog.table_exists(table_id), f"{table_id} doesn't exist"
        t = self._catalog.load_table(table_id)
        for c in column_names:
            assert c in t.schema().column_names

    def assert_has_n_rows(self, table_id: tuple[str, ...], n: int):
        assert self.get_num_rows(table_id) == n

    def get_num_rows(self, table_id: tuple[str, ...]):
        assert self._catalog.table_exists(table_id)
        t = self._catalog.load_table(table_id)
        snapshot = t.current_snapshot()
        assert snapshot is not None
        assert snapshot.summary is not None
        return int(snapshot.summary.additional_properties["total-records"])


@pytest.fixture(scope="session")
def sql_warehouses(request, tmp_path_factory):
    test_dir = tmp_path_factory.mktemp("warehouses")
    test_paths = (t.path for t in request.session.items)
    test_warehouse_names = {_get_warehouse_name_from_test_filepath(tp) for tp in test_paths}
    LOGGER.debug(f"Creating {test_warehouse_names} warehouses in {test_dir}")

    warehouses = {
        warehouse_name: SqlCatalogWarehouse(warehouse_name, test_dir)
        for warehouse_name in test_warehouse_names
    }

    return warehouses


@pytest.fixture
def test_catalog(sql_warehouses, request, monkeypatch):
    test_warehouse_name = _get_warehouse_name_from_test_filepath(request.path)
    warehouse = sql_warehouses[test_warehouse_name]

    monkeypatch.setenv("PYICEBERG_CATALOG__DEFAULT__TYPE", "sql")
    monkeypatch.setenv("PYICEBERG_CATALOG__DEFAULT__URI", warehouse.uri)
    monkeypatch.setenv("PYICEBERG_CATALOG__DEFAULT__WAREHOUSE", test_warehouse_name)

    catalog = warehouse.connect()

    try:
        yield AssertableCatalog(catalog)
    finally:
        catalog.close()


@pytest.fixture
def run_test_ingest(request, monkeypatch):
    w = _get_warehouse_name_from_test_filepath(request.path)
    manifest = create_ingest_manifest(w, request.path.parent)

    def run(**config_vars):
        LOGGER.debug(f"Running ingest for manifest {manifest} with config vars {config_vars}")
        for k, v in config_vars.items():
            monkeypatch.setenv(f"{manifest.name}__{k}", v)

        run_ingest(manifest)

    return run


def _get_warehouse_name_from_test_filepath(fp: Path):
    """Relies on the test file being '<warehouse>/ingest/<domain>/<job>/test_<job>.py'"""
    return fp.parent.parent.parent.parent.name
