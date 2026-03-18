import pyiceberg.transforms as transforms
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import (
    UNSORTED_SORT_ORDER,
    SortOrder,
    SortField,
    SortDirection,
)

SortOrderHint = dict[str, str] | None


class SortOrderSpecification:
    direction: str
    """The direction to apply to the sort"""
    column_name: str
    """Column name to apply the transformation to"""

    def __init__(self, direction: str, column_name: str) -> None:
        self.direction = direction
        self.column_name = column_name


class SortOrderBuilder:
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

    def asc(self) -> "SortOrderBuilder":
        self._direction = SortDirection.ASC.value
        return self

    def desc(self) -> "SortOrderBuilder":
        self._direction = SortDirection.DESC.value
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
    #     return SortOrderSpecification(transforms.IDENTITY, column_name)


def create_sort_order(sort_order_hint: SortOrderHint, iceberg_schema: Schema) -> SortOrder:
    """If the table specifies hints to a Iceberg sort order, create the appropriate
    SortOrder instance.
    """
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
