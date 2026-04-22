"""Tests for elt_common.iceberg.partition"""

import pytest
from elt_common.iceberg.partition import create_partition_spec
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, DateType, TimestamptzType
from pyiceberg import transforms


@pytest.fixture(scope="session")
def sample_schema():
    """Create a sample Iceberg schema for testing."""
    return Schema(
        NestedField(field_id=1, name="id", type=LongType(), required=True),
        NestedField(field_id=2, name="name", type=StringType(), required=True),
        NestedField(field_id=3, name="date_col", type=DateType(), required=False),
        NestedField(field_id=4, name="ts_col", type=TimestamptzType(), required=False),
    )


class TestCreatePartitionSpecNone:
    """Tests for None and empty partition hints."""

    def test_create_partition_spec_when_hint_none(self, sample_schema):
        """Test that None partition hint returns unpartitioned spec."""
        result = create_partition_spec(None, sample_schema)
        assert result == UNPARTITIONED_PARTITION_SPEC

    def test_create_partition_spec_when_hint_empty(self, sample_schema):
        """Test that empty dict partition hint returns unpartitioned spec."""
        result = create_partition_spec({}, sample_schema)
        assert result == UNPARTITIONED_PARTITION_SPEC


class TestCreatePartitionSpecSingleColumn:
    """Tests for single column partitioning with various transforms."""

    def test_create_partition_spec_single_column_identity(self, sample_schema):
        """Test single column with identity transform."""
        partition_hint = {"name": "identity"}
        result = create_partition_spec(partition_hint, sample_schema)

        assert len(result.fields) == 1
        field = result.fields[0]
        assert field.name == "name_identity"
        assert field.source_id == 2  # id of 'name' field
        assert field.field_id == 1000
        assert isinstance(field.transform, transforms.IdentityTransform)

    @pytest.mark.parametrize(
        "transform,expected_name,expected_transform_type",
        [
            ("year", "date_col_year", transforms.YearTransform),
            ("month", "date_col_month", transforms.MonthTransform),
            ("day", "date_col_day", transforms.DayTransform),
        ],
    )
    def test_create_partition_spec_single_column_date_transforms(
        self, sample_schema, transform, expected_name, expected_transform_type
    ):
        """Test single column with date transforms (year, month, day)."""
        partition_hint = {"date_col": transform}
        result = create_partition_spec(partition_hint, sample_schema)

        assert len(result.fields) == 1
        field = result.fields[0]
        assert field.name == expected_name
        assert field.source_id == 3  # id of 'date_col' field
        assert field.field_id == 1000
        assert isinstance(field.transform, expected_transform_type)

    def test_create_partition_spec_single_column_hour(self, sample_schema):
        """Test single column with hour transform."""
        partition_hint = {"ts_col": "hour"}
        result = create_partition_spec(partition_hint, sample_schema)

        assert len(result.fields) == 1
        field = result.fields[0]
        assert field.name == "ts_col_hour"
        assert field.source_id == 4  # id of 'ts_col' field
        assert field.field_id == 1000
        assert isinstance(field.transform, transforms.HourTransform)


class TestCreatePartitionSpecParameterizedTransform:
    """Tests for transforms with parameters."""

    def test_create_partition_spec_truncate_transform(self, sample_schema):
        """Test transform with parameter like truncate[10]."""
        partition_hint = {"name": "truncate[10]"}
        result = create_partition_spec(partition_hint, sample_schema)

        assert len(result.fields) == 1
        field = result.fields[0]
        # Field name should use only the transform part before bracket
        assert field.name == "name_truncate"
        assert field.source_id == 2
        assert isinstance(field.transform, transforms.TruncateTransform)

    def test_create_partition_spec_bucket_transform(self, sample_schema):
        """Test bucket transform with parameter."""
        partition_hint = {"id": "bucket[100]"}
        result = create_partition_spec(partition_hint, sample_schema)

        assert len(result.fields) == 1
        field = result.fields[0]
        assert field.name == "id_bucket"
        assert field.source_id == 1
        assert isinstance(field.transform, transforms.BucketTransform)


class TestCreatePartitionSpecMultipleColumns:
    """Tests for partitioning with multiple columns."""

    def test_create_partition_spec_multiple_columns(self, sample_schema):
        """Test multiple columns with different transforms."""
        partition_hint = {
            "date_col": "year",
            "name": "identity",
            "ts_col": "month",
        }
        result = create_partition_spec(partition_hint, sample_schema)

        assert len(result.fields) == 3

        # Check field IDs are sequential starting from 1000
        field_ids = sorted([field.field_id for field in result.fields])
        assert field_ids == [1000, 1001, 1002]

        # Check field names
        field_names = sorted([field.name for field in result.fields])
        assert "date_col_year" in field_names
        assert "name_identity" in field_names
        assert "ts_col_month" in field_names

        # Check transform types
        transform_map = {field.name: field.transform for field in result.fields}
        assert isinstance(transform_map["date_col_year"], transforms.YearTransform)
        assert isinstance(transform_map["name_identity"], transforms.IdentityTransform)
        assert isinstance(transform_map["ts_col_month"], transforms.MonthTransform)

    def test_create_partition_spec_multiple_columns_field_id_sequence(self, sample_schema):
        """Test that field IDs are assigned sequentially."""
        partition_hint = {
            "id": "identity",
            "name": "identity",
            "date_col": "year",
        }
        result = create_partition_spec(partition_hint, sample_schema)

        # Collect field IDs and check they're sequential
        field_ids = [field.field_id for field in result.fields]
        assert len(set(field_ids)) == 3  # All unique
        assert all(field_id >= 1000 for field_id in field_ids)


class TestCreatePartitionSpecErrors:
    """Tests for edge cases and error scenarios."""

    def test_create_partition_spec_nonexistent_column_raises_error(self, sample_schema):
        """Test that referencing a nonexistent column raises an error."""
        partition_hint = {"nonexistent_col": "year"}

        with pytest.raises(Exception):  # pyiceberg raises an error for missing fields
            create_partition_spec(partition_hint, sample_schema)
