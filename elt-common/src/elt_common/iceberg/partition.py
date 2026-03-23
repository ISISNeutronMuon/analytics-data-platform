from elt_common.typing import PartitionHint
from pyiceberg.partitioning import (
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema
import pyiceberg.transforms as transforms


def create_partition_spec(
    partition_hint: PartitionHint | None, iceberg_schema: Schema
) -> PartitionSpec:
    """Create an Iceberg partition spec rhe partition hints"""

    def field_name(column_name: str, transform: str):
        bracket_index = transform.find("[")
        return f"{column_name}_{transform[:bracket_index] if bracket_index > 0 else transform}"

    if partition_hint is None:
        return UNPARTITIONED_PARTITION_SPEC

    return PartitionSpec(
        *(
            PartitionField(
                source_id=iceberg_schema.find_field(column_name).field_id,
                field_id=1000 + index,  # the documentation does this...
                transform=transforms.parse_transform(transform),
                name=field_name(column_name, transform),
            )
            for index, (column_name, transform) in enumerate(partition_hint.items())
        )
    )
