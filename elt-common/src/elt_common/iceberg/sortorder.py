from elt_common.typing import SortOrderConfig
import pyiceberg.transforms as transforms
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import (
    UNSORTED_SORT_ORDER,
    SortOrder,
    SortField,
    SortDirection,
)


def create_sort_order(
    sort_order_config: SortOrderConfig | None, iceberg_schema: Schema
) -> SortOrder:
    """If a sort order hint is provider, create the appropriate SortOrder instance."""
    if not sort_order_config:
        return UNSORTED_SORT_ORDER

    return SortOrder(
        *(
            SortField(
                source_id=iceberg_schema.find_field(column_name).field_id,
                direction=SortDirection(direction),
                transform=transforms.parse_transform("identity"),
            )
            for column_name, direction in sort_order_config.items()
        )
    )
