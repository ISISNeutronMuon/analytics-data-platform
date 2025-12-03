import datetime as dt

from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    PartitionTrBuilder,
    SortOrderBuilder,
)
from elt_common.dlt_destinations.pyiceberg.helpers import PARTITION_HINT, SORT_ORDER_HINT
import pytest


def test_pyiceberg_adapter_requires_one_of_partition_or_sort_order():
    with pytest.raises(ValueError):
        pyiceberg_adapter(data=[{"entry_id": 100}])


# Full tests are performed during the e2e tests in test_pyiceberg_pipeline.py
def test_pyiceberg_adapter_sets_partition_hints_if_partitions_supplied():
    resource = pyiceberg_adapter(
        data=[{"entry_id": 100, "entry_timestamp": dt.datetime.now()}],
        partition=PartitionTrBuilder.year("entry_timestamp"),
    )
    dlt_schema = resource.compute_table_schema()

    assert PARTITION_HINT in dlt_schema
    assert "entry_timestamp" in dlt_schema[PARTITION_HINT]
    assert dlt_schema[PARTITION_HINT]["entry_timestamp"] == "year"


def test_pyiceberg_adapter_sets_sortorder_if_order_supplied():
    resource = pyiceberg_adapter(
        data=[{"entry_id": 100, "entry_timestamp": dt.datetime.now()}],
        sort_order=SortOrderBuilder("entry_id").asc().build(),
    )
    dlt_schema = resource.compute_table_schema()

    assert SORT_ORDER_HINT in dlt_schema
    assert "entry_id" in dlt_schema[SORT_ORDER_HINT]
    assert dlt_schema[SORT_ORDER_HINT]["entry_id"] == "asc"
