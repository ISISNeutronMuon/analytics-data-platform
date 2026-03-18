import pyiceberg.transforms as transforms
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import (
    UNSORTED_SORT_ORDER,
    SortOrder,
    SortField,
    SortDirection,
)

SortOrderHint = dict[str, str] | None


def create_sort_order(sort_order_hint: SortOrderHint, iceberg_schema: Schema) -> SortOrder:
    """If a sort order hint is provider, create the appropriate SortOrder instance."""
    if sort_order_hint is None:
        return UNSORTED_SORT_ORDER

    return SortOrder(
        *(
            SortField(
                source_id=iceberg_schema.find_field(column_name).field_id,
                direction=SortDirection(direction),
                transform=transforms.parse_transform("identity"),
            )
            for column_name, direction in sort_order_hint.items()
        )
    )
