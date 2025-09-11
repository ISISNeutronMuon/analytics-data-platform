from tempfile import TemporaryDirectory

import pytest


from e2e_tests.pipelines.destinations.pyiceberg.utils import (
    PyIcebergDestinationTestConfiguration,
)


@pytest.fixture
def pipelines_dir():
    with TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def destination_config():
    destination_config = PyIcebergDestinationTestConfiguration()
    try:
        yield destination_config
    finally:
        destination_config.clean_catalog()
