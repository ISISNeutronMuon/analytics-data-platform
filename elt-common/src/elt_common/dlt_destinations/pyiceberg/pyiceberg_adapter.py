from typing import Any, Dict, Sequence, Union, Final, TypeAlias

from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate

import pyiceberg.table.sorting as sorting
import pyiceberg.transforms as tr

PARTITION_HINT: Final[str] = "x-pyiceberg-partition"
SORT_ORDER_HINT: Final[str] = "x-pyiceberg-sortorder"

# This module is a copy of the athena_adapter from
# dlt/destinations/impl/athena/athena_adatper.py adapted to the pyiceberg
# destination.


class PartitionTransformation:
    transform: str
    """The transform as a string representation understood by pyicberg.transforms.parse_transform., e.g. `bucket[16]`"""
    column_name: str
    """Column name to apply the transformation to"""

    def __init__(self, transform: str, column_name: str) -> None:
        self.transform = transform
        self.column_name = column_name


class pyiceberg_partition:
    """Helper class to generate iceberg partition transformations"""

    @staticmethod
    def identity(column_name: str) -> PartitionTransformation:
        """Partition by column without an transformation"""
        return PartitionTransformation(tr.IDENTITY, column_name)

    @staticmethod
    def year(column_name: str) -> PartitionTransformation:
        """Partition by year part of a date or timestamp column."""
        return PartitionTransformation(tr.YEAR, column_name)

    @staticmethod
    def month(column_name: str) -> PartitionTransformation:
        """Partition by month part of a date or timestamp column."""
        return PartitionTransformation(tr.MONTH, column_name)

    @staticmethod
    def day(column_name: str) -> PartitionTransformation:
        """Partition by day part of a date or timestamp column."""
        return PartitionTransformation(tr.DAY, column_name)

    @staticmethod
    def hour(column_name: str) -> PartitionTransformation:
        """Partition by hour part of a date or timestamp column."""
        return PartitionTransformation(tr.HOUR, column_name)

    # NOTE: The following transformations are not currently supported by writing through
    # pyarrow so they are disabled.

    # @staticmethod
    # def bucket(n: int, column_name: str) -> PartitionTransformation:
    #     """Partition by hashed value to n buckets."""
    #     return PartitionTransformation(f"{tr.BUCKET}[{n}]", column_name)

    # @staticmethod
    # def truncate(length: int, column_name: str) -> PartitionTransformation:
    #     """Partition by value truncated to length."""
    #     return PartitionTransformation(f"{tr.TRUNCATE}[{length}]", column_name)


class SortOrderSpecification:
    direction: str
    """The direction to apply to the sort"""
    column_name: str
    """Column name to apply the transformation to"""

    def __init__(self, direction: str, column_name: str) -> None:
        self.direction = direction
        self.column_name = column_name


class pyiceberg_sortorder:
    """Builder to generate iceberg sort order specs.

    Note: This only affects the order in which the data is written and not the final
    query. Queries still need to include any ORDER BY clauses if necessary.
    """

    def __init__(self, column_name: str) -> None:
        self.column_name = column_name
        self._direction = None

    @property
    def direction(self) -> str:
        if self._direction is None:
            raise ValueError(
                "Sort direction not specified. Use .asc()/.desc() to indicate the required sort direction."
            )
        return self._direction

    def asc(self) -> "pyiceberg_sortorder":
        self._direction = sorting.SortDirection.ASC.value
        return self

    def desc(self) -> "pyiceberg_sortorder":
        self._direction = sorting.SortDirection.DESC.value
        return self

    def build(self) -> SortOrderSpecification:
        return SortOrderSpecification(self.direction, self.column_name)

    # @staticmethod
    # def ascending(transform: PartitionTransformation) -> SortOrderSpecification:
    # @staticmethod
    # def identity(
    #     column_name: str, direction: str, null_order: str
    # ) -> SortOrderSpecification:
    #     """Sort by a column without a transformation"""
    #     return SortOrderSpecification(tr.IDENTITY, column_name)


TPartitionTransformation: TypeAlias = Union[
    str, PartitionTransformation, Sequence[Union[str, PartitionTransformation]]
]
TSortOrderConfiguration: TypeAlias = Union[SortOrderSpecification, Sequence[SortOrderSpecification]]


def pyiceberg_adapter(
    data: Any,
    partition: TPartitionTransformation | None = None,
    sort_order: TSortOrderConfiguration | None = None,
) -> DltResource:
    """
    Prepares data for loading into pyiceberg

    Args:
        data: The data to be transformed.
            This can be raw data or an instance of DltResource.
            If raw data is provided, the function will wrap it into a `DltResource` object.
        partition: Column name(s) or instances of `PartitionTransformation` to partition the table by.
        sort_order: Column name(s) or instances of `SortOrderConfiguration` to apply to the table.

    Returns:
        A `DltResource` object that is ready to be loaded into a pyiceberg catalog.

    Raises:
        ValueError: If any hint is invalid or none are specified.

    Examples:
        >>> data = [{"name": "Marcel", "department": "Engineering", "date_hired": "2024-01-30"}]
        >>> pyiceberg_adapter(data, partition=["department", pyiceberg_partition.year("date_hired"), pyiceberg_partition.bucket(8, "name")])
        [DltResource with hints applied]
    """
    resource = get_resource_for_adapter(data)
    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}

    if partition:
        if isinstance(partition, str) or not isinstance(partition, Sequence):
            partition = [partition]

        # Partition hint is `{column_name: PartitionTransformation}`
        # Use one dict for all hints instead of storing on column so order is preserved
        partition_hint: Dict[str, str] = {}
        for item in partition:
            # client understand how to decode this representation into the correct PartitionField specs
            if isinstance(item, PartitionTransformation):
                partition_hint[item.column_name] = item.transform
            else:
                partition_hint[item] = pyiceberg_partition.identity(item).transform

        additional_table_hints[PARTITION_HINT] = partition_hint

    if sort_order:
        if not isinstance(sort_order, Sequence):
            sort_order = [sort_order]

        # Use one dict for all hints instead of storing on column so order is preserved
        sort_order_hint: Dict[str, str] = {}
        for item in sort_order:
            sort_order_hint[item.column_name] = item.direction

        additional_table_hints[SORT_ORDER_HINT] = sort_order_hint

    if additional_table_hints:
        resource.apply_hints(additional_table_hints=additional_table_hints)
    else:
        raise ValueError("A value for `partition` or `sort_order` must be specified.")

    return resource
