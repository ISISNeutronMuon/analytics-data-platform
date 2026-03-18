from pyiceberg.partitioning import (
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema
import pyiceberg.transforms as transforms

PartitionHint = dict[str, str] | None


class PartitionTransformation:
    transform: str
    """The transform as a string representation understood by pyicberg.transforms.parse_transform., e.g. `bucket[16]`"""
    column_name: str
    """Column name to apply the transformation to"""

    def __init__(self, transform: str, column_name: str) -> None:
        self.transform = transform
        self.column_name = column_name


class PartitionTrBuilder:
    """Helper class to generate iceberg partition transformations"""

    @staticmethod
    def identity(column_name: str) -> PartitionTransformation:
        """Partition by column without an transformation"""
        return PartitionTransformation(transforms.IDENTITY, column_name)

    @staticmethod
    def year(column_name: str) -> PartitionTransformation:
        """Partition by year part of a date or timestamp column."""
        return PartitionTransformation(transforms.YEAR, column_name)

    @staticmethod
    def month(column_name: str) -> PartitionTransformation:
        """Partition by month part of a date or timestamp column."""
        return PartitionTransformation(transforms.MONTH, column_name)

    @staticmethod
    def day(column_name: str) -> PartitionTransformation:
        """Partition by day part of a date or timestamp column."""
        return PartitionTransformation(transforms.DAY, column_name)

    @staticmethod
    def hour(column_name: str) -> PartitionTransformation:
        """Partition by hour part of a date or timestamp column."""
        return PartitionTransformation(transforms.HOUR, column_name)

    # NOTE: The following transformations are not currently supported by writing through
    # pyarrow so they are disabled.

    # @staticmethod
    # def bucket(n: int, column_name: str) -> PartitionTransformation:
    #     """Partition by hashed value to n buckets."""
    #     return PartitionTransformation(f"{transforms.BUCKET}[{n}]", column_name)

    # @staticmethod
    # def truncate(length: int, column_name: str) -> PartitionTransformation:
    #     """Partition by value truncated to length."""
    #     return PartitionTransformation(f"{transforms.TRUNCATE}[{length}]", column_name)


def create_partition_spec(partition_hint: PartitionHint, iceberg_schema: Schema) -> PartitionSpec:
    """Create an Iceberg partition spec for this table if the partition hints
    have been provided

    :param partition_hint: An optional dict mapping column names to iceberg transformations
    """

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
