from contextlib import contextmanager
from dataclasses import dataclass
import os
from typing import cast, Any, Dict, List, Union

import dlt
from dlt.common.pendulum import pendulum
from dlt.common.destination import (
    TDestinationReferenceArg,
)
from dlt.common.schema.typing import TTableSchema
from dlt.common.destination.exceptions import SqlClientNotAvailable

from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection
from pyiceberg.transforms import (
    IdentityTransform,
    YearTransform,
    MonthTransform,
    DayTransform,
    HourTransform,
)

from elt_common.dlt_destinations.pyiceberg.pyiceberg import (
    PyIcebergClient,
)
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_partition,
    pyiceberg_sortorder,
    PartitionTransformation,
    SortOrderSpecification,
)

from e2e_tests.conftest import Warehouse


@dataclass
class PyIcebergDestinationTestConfiguration:
    """Class for defining test setup for pyiceberg destination."""

    destination: TDestinationReferenceArg = "elt_common.dlt_destinations.pyiceberg"

    def setup(self, warehouse: Warehouse) -> None:
        """Sets up environment variables for this destination configuration"""
        server, server_settings = warehouse.server, warehouse.server.settings
        os.environ["DESTINATION__PYICEBERG__CREDENTIALS__URI"] = str(
            warehouse.server.catalog_endpoint()
        )
        os.environ.setdefault("DESTINATION__PYICEBERG__CREDENTIALS__WAREHOUSE", warehouse.name)
        os.environ.setdefault(
            "DESTINATION__PYICEBERG__CREDENTIALS__OAUTH2_SERVER_URI",
            str(server.token_endpoint),
        )
        os.environ.setdefault(
            "DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_ID",
            server_settings.openid_client_id,
        )
        os.environ.setdefault(
            "DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_SECRET",
            server_settings.openid_client_secret,
        )
        os.environ.setdefault(
            "DESTINATION__PYICEBERG__CREDENTIALS__SCOPE",
            server_settings.openid_scope,
        )
        os.environ.setdefault("DESTINATION__PYICEBERG__BUCKET_URL", warehouse.bucket_url)
        # Avoid collisons on table names when the same ones are created/deleted in quick
        # succession
        os.environ.setdefault(
            "DESTINATION__PYICEBERG__TABLE_LOCATION_LAYOUT",
            "{location_tag}/{dataset_name}/{table_name}",
        )

    def setup_pipeline(
        self,
        warehouse: Warehouse,
        pipeline_name: str,
        dataset_name: str = None,
        dev_mode: bool = False,
        **kwargs,
    ) -> dlt.Pipeline:
        """Convenience method to setup pipeline with this configuration"""
        self.setup(warehouse)
        self.active_pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            pipelines_dir=kwargs.pop("pipelines_dir", None),
            destination=self.destination,
            dataset_name=(dataset_name if dataset_name is not None else pipeline_name + "_data"),
            dev_mode=dev_mode,
            **kwargs,
        )
        return cast(dlt.Pipeline, self.active_pipeline)

    def clean_catalog(self):
        """Clean the destination catalog of all namespaces and tables"""
        pipeline = self.active_pipeline
        if pipeline is None:
            return

        with pipeline.destination_client() as client:
            catalog = cast(PyIcebergClient, client).iceberg_catalog
            for ns_name in catalog.list_namespaces():
                tables = catalog.list_tables(ns_name)
                for qualified_table_name in tables:
                    catalog.purge_table(qualified_table_name)
                catalog.drop_namespace(ns_name)

    def attach_pipeline(self, pipeline_name: str, **kwargs) -> dlt.Pipeline:
        """Attach to existing pipeline keeping the dev_mode"""
        # remember dev_mode from setup_pipeline
        pipeline = dlt.attach(pipeline_name, **kwargs)
        return pipeline

    def supports_sql_client(self, pipeline: dlt.Pipeline) -> bool:
        """Checks if destination supports SQL queries"""
        try:
            pipeline.sql_client()
            return True
        except SqlClientNotAvailable:
            return False

    @property
    def active_pipeline(self) -> dlt.Pipeline | None:
        return self._active_pipeline

    @active_pipeline.setter
    def active_pipeline(self, value: dlt.Pipeline):
        self._active_pipeline = value


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
                partition_request=[getattr(pyiceberg_partition, dt_element)("created_at")],
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
            partition_request=["category", pyiceberg_partition.year("created_at")],
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
            sort_order_request=[pyiceberg_sortorder("created_at").desc().build()],
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
                pyiceberg_sortorder("category").asc().build(),
                pyiceberg_sortorder("created_at").desc().build(),
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
    assert arrow_table.shape[0] == expected_items_count

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


@contextmanager
def iceberg_catalog(pipeline: dlt.Pipeline):
    with pipeline.destination_client() as client:
        yield cast(PyIcebergClient, client).iceberg_catalog
