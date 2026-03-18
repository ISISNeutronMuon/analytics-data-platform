from elt_common.iceberg.partition import create_partition_spec
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema


def test_create_partition_spec_when_hint_none_or_empty():
    schema = Schema()
    assert create_partition_spec(None, schema) == UNPARTITIONED_PARTITION_SPEC
    assert create_partition_spec({}, schema) == UNPARTITIONED_PARTITION_SPEC
