from elt_common.testing.lakekeeper import Settings
import pytest


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()
