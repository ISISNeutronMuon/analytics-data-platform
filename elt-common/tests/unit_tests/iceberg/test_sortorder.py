import pytest
from elt_common.iceberg.sortorder import create_sort_order
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortDirection


def test_create_sort_order_when_config_empty_or_none(sample_schema):
    assert create_sort_order(None, sample_schema) == UNSORTED_SORT_ORDER
    assert create_sort_order({}, sample_schema) == UNSORTED_SORT_ORDER


@pytest.mark.parametrize(
    "sort_config,expected_source_id,expected_direction",
    [
        ({"id": "asc"}, 1, SortDirection.ASC),
        ({"name": "desc"}, 2, SortDirection.DESC),
    ],
)
def test_create_sort_order_single_column(
    sample_schema, sort_config, expected_source_id, expected_direction
):
    result = create_sort_order(sort_config, sample_schema)

    assert len(result.fields) == 1
    field = result.fields[0]
    assert field.source_id == expected_source_id
    assert field.direction == expected_direction


def test_create_sort_order_multiple_columns(sample_schema):
    sort_config = {"id": "asc", "name": "desc", "date_col": "asc"}
    result = create_sort_order(sort_config, sample_schema)

    assert len(result.fields) == 3
    # Check that all columns are present
    source_ids = {field.source_id for field in result.fields}
    assert source_ids == {1, 2, 3}  # id, name, date_col


@pytest.mark.parametrize(
    "sort_config",
    [
        {"nonexistent_col": "asc"},  # invalid column name
        {"id": "invalid"},  # invalid direction value
    ],
)
def test_create_sort_order_invalid_config(sample_schema, sort_config):
    """Test that invalid column names and directions raise errors."""
    with pytest.raises(Exception):
        create_sort_order(sort_config, sample_schema)
