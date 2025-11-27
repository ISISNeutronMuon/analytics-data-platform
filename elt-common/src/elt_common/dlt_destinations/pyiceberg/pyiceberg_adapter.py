from typing import Any, Dict, Sequence, Union, TypeAlias

from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource
from dlt.common.typing import TTableHintTemplate

from .helpers import (
    PARTITION_HINT,
    SORT_ORDER_HINT,
    PartitionTransformation,
    PartitionTrBuilder,
    SortOrderSpecification,
    SortOrderBuilder,  # noqa: F401
)

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
        >>> pyiceberg_adapter(data, partition=["department", PartitionTrBuilder.year("date_hired")])
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
            # client understands how to decode this representation into the correct PartitionField specs
            if isinstance(item, PartitionTransformation):
                partition_hint[item.column_name] = item.transform
            else:
                partition_hint[item] = PartitionTrBuilder.identity(item).transform

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
