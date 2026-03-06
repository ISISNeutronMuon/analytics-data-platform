import pathlib
import sys

from elt_common.testing.lakekeeper import Settings
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent))


def pytest_addoption(parser):
    parser.addoption(
        "--catalog-type",
        action="store",
        default="rest",
        help="Choose the type of catalog to run tests against. Options=sql,rest",
    )


@pytest.fixture(scope="session")
def settings(request) -> Settings:
    settings = Settings()
    if (catalog_type := request.config.getoption("--catalog-type")) is not None:
        settings.catalog_type = catalog_type

    return settings
