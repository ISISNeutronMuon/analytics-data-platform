"""Common fixtures and pytest config"""

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DateType, TimestamptzType
import pytest


@pytest.fixture(scope="session")
def sample_schema():
    """Create a sample Iceberg schema for testing."""
    return Schema(
        NestedField(field_id=1, name="id", type=LongType(), required=True),
        NestedField(field_id=2, name="name", type=StringType(), required=True),
        NestedField(field_id=3, name="date_col", type=DateType(), required=False),
        NestedField(field_id=4, name="ts_col", type=TimestamptzType(), required=False),
    )
