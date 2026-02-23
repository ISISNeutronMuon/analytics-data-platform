from elt_common.testing.dlt import configure_dlt_for_testing
from elt_common.testing.lakekeeper import Settings
import pytest


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


configure_dlt_for_testing()
