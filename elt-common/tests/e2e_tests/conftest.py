from enum import StrEnum
import os
from pathlib import Path

from elt_common.iceberg.catalog import connect_catalog
import pytest


class CatalogType(StrEnum):
    REST = "rest"
    SQL = "sql"


def pytest_addoption(parser):
    parser.addoption(
        "--catalog-type",
        action="store",
        default=CatalogType.SQL,
        help=f"Choose the type of catalog to run tests against. Supported values={[e.value for e in CatalogType]}",
    )


@pytest.fixture
def catalog(request, tmp_path: Path):
    catalog_type = request.config.getoption("--catalog-type")
    if catalog_type.lower() == CatalogType.SQL:
        os.environ["PYICEBERG_CATALOG__DEFAULT__TYPE"] = "sql"
        os.environ["PYICEBERG_CATALOG__DEFAULT__URI"] = f"sqlite:///{tmp_path}/default.db"
        os.environ["PYICEBERG_CATALOG__DEFAULT__WAREHOUSE"] = f"file://{tmp_path}/default"
    else:
        # The values below are hard coded to the docker-based setup in local/infra.
        base_addr = "http://localhost:50080"
        os.environ["PYICEBERG_CATALOG__DEFAULT__TYPE"] = "rest"
        os.environ["PYICEBERG_CATALOG__DEFAULT__URI"] = f"{base_addr}/iceberg/catalog"
        os.environ["PYICEBERG_CATALOG__DEFAULT__WAREHOUSE"] = "facility_ops_landing"
        os.environ["PYICEBERG_CATALOG__DEFAULT__CREDENTIAL"] = "machine-infra:s3cr3t"
        os.environ["PYICEBERG_CATALOG__DEFAULT__OAUTH2_SERVER_URI"] = (
            f"{base_addr}/auth/realms/analytics-data-platform/protocol/openid-connect/token"
        )
        os.environ["PYICEBERG_CATALOG__DEFAULT__SCOPE"] = "lakekeeper"

    yield connect_catalog()
