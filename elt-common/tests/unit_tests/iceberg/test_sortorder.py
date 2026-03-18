from elt_common.iceberg.sortorder import create_sort_order
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.schema import Schema


def test_create_sort_order_when_hint_empty_or_none(self):
    schema = Schema()
    assert create_sort_order(None, schema) == UNSORTED_SORT_ORDER
    assert create_sort_order({}, schema) == UNSORTED_SORT_ORDER
