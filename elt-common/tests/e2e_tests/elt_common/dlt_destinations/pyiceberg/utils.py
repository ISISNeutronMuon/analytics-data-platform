from dataclasses import dataclass
from typing import Any, Dict, List, Union

import dlt
from dlt.common.schema.typing import TTableSchema
import pendulum
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection
from pyiceberg.transforms import (
    IdentityTransform,
    YearTransform,
    MonthTransform,
    DayTransform,
    HourTransform,
)

from elt_common.dlt_destinations.pyiceberg import iceberg_catalog

from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    PartitionTrBuilder,
    SortOrderBuilder,
    PartitionTransformation,
    SortOrderSpecification,
)


@dataclass
class PyIcebergPartitionTestConfiguration:
    name: str
    data: List[Dict[str, Any]]
    partition_request: List[Union[str, PartitionTransformation]]
    expected_spec: PartitionSpec


@dataclass
class PyIcebergSortOrderTestConfiguration:
    name: str
    data: List[Dict[str, Any]]
    sort_order_request: List[SortOrderSpecification]
    expected_spec: SortOrder


def partition_test_configs() -> List[PyIcebergPartitionTestConfiguration]:
    standard_test_data = [
        {"id": i, "category": c, "created_at": d}
        for i, c, d in [
            (1, "A", pendulum.datetime(2020, 1, 1, 9, 15, 20)),
            (2, "B", pendulum.datetime(2021, 1, 1, 10, 40, 30)),
        ]
    ]
    test_configs = [
        PyIcebergPartitionTestConfiguration(
            name="partition_by_identity",
            data=standard_test_data,
            partition_request=["category"],
            expected_spec=PartitionSpec(
                PartitionField(
                    source_id=2,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="category_identity",
                )
            ),
        )
    ]
    # add date/time-based partitions
    test_configs.extend(
        [
            PyIcebergPartitionTestConfiguration(
                name=f"partition_date_by_{dt_element}",
                data=standard_test_data,
                partition_request=[getattr(PartitionTrBuilder, dt_element)("created_at")],
                expected_spec=PartitionSpec(
                    PartitionField(
                        source_id=3,
                        field_id=1000,
                        transform=expected_transform,
                        name=f"created_at_{dt_element}",
                    ),
                ),
            )
            for dt_element, expected_transform in [
                ("year", YearTransform()),
                ("month", MonthTransform()),
                ("day", DayTransform()),
                ("hour", HourTransform()),
            ]
        ]
    )
    # bucket & truncatetransforms are not currently supported.
    # See note in pyiceberg_adapter.pyiceberg_partition class

    # check multiple partition fields
    test_configs.append(
        PyIcebergPartitionTestConfiguration(
            name="partition_by_multiple_fields",
            data=standard_test_data,
            partition_request=["category", PartitionTrBuilder.year("created_at")],
            expected_spec=PartitionSpec(
                PartitionField(
                    source_id=2,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="category_identity",
                ),
                PartitionField(
                    source_id=3,
                    field_id=1001,
                    transform=YearTransform(),
                    name="created_at_year",
                ),
            ),
        )
    )

    return test_configs


def sort_order_test_configs() -> List[PyIcebergSortOrderTestConfiguration]:
    test_configs: List[PyIcebergSortOrderTestConfiguration] = []

    standard_test_data = [
        {"id": i, "category": c, "created_at": d}
        for i, c, d in [
            (1, "A", pendulum.datetime(2020, 1, 1, 9, 15, 20)),
            (2, "B", pendulum.datetime(2021, 1, 1, 10, 40, 30)),
        ]
    ]
    test_configs = [
        PyIcebergSortOrderTestConfiguration(
            name="sort_by_single_field",
            data=standard_test_data,
            sort_order_request=[SortOrderBuilder("created_at").desc().build()],
            expected_spec=SortOrder(
                SortField(
                    source_id=3,
                    transform=IdentityTransform(),
                    direction=SortDirection.DESC,
                )
            ),
        ),
        PyIcebergSortOrderTestConfiguration(
            name="sort_by_multiple_fields",
            data=standard_test_data,
            sort_order_request=[
                SortOrderBuilder("category").asc().build(),
                SortOrderBuilder("created_at").desc().build(),
            ],
            expected_spec=SortOrder(
                *[
                    SortField(
                        source_id=2,
                        transform=IdentityTransform(),
                        direction=SortDirection.ASC,
                    ),
                    SortField(
                        source_id=3,
                        transform=IdentityTransform(),
                        direction=SortDirection.DESC,
                    ),
                ],
            ),
        ),
    ]

    return test_configs


def assert_table_has_shape(
    pipeline: dlt.Pipeline,
    qualified_table_name: str,
    *,
    expected_row_count: int,
    expected_schema: TTableSchema,
):
    with iceberg_catalog(pipeline) as catalog:
        assert catalog.table_exists(qualified_table_name)
        table = catalog.load_table(qualified_table_name)
        assert table.scan().to_arrow().shape[0] == expected_row_count
        table_columns = table.schema().column_names
        for column_name in expected_schema.get("columns", ["No columns field!"]):
            assert column_name in table_columns


def assert_table_has_data(
    pipeline: dlt.Pipeline,
    qualified_table_name: str,
    *,
    expected_items_count: int,
    items: List[Any] = None,
):
    with iceberg_catalog(pipeline) as catalog:
        assert catalog.table_exists(qualified_table_name)
        table = catalog.load_table(qualified_table_name)

    arrow_table = table.scan().to_arrow()
    assert arrow_table.shape[0] == expected_items_count, (
        f"{arrow_table.shape[0]} != {expected_items_count}"
    )

    if items is None:
        return

    drop_keys = ["_dlt_id", "_dlt_load_id"]
    objects_without_dlt_keys = [
        {k: v for k, v in record.items() if k not in drop_keys}
        for record in arrow_table.to_pylist()
    ]

    assert_unordered_list_equal(objects_without_dlt_keys, items)


def assert_unordered_list_equal(list1: List[Any], list2: List[Any]) -> None:
    assert len(list1) == len(list2), "Lists have different length"
    for item in list1:
        assert item in list2, f"Item {item} not found in list2"
