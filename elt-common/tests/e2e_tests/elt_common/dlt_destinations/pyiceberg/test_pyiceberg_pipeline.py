import inspect
import shutil
from types import FrameType
from typing import Any, Dict, List

import dlt
from dlt.common.schema.utils import loads_table, pipeline_state_table, version_table
from elt_common.dlt_destinations.pyiceberg import iceberg_catalog
from elt_common.dlt_destinations.pyiceberg.helpers import (
    namespace_exists as catalog_namespace_exists,
)
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
)
from elt_common.testing.dlt import PyIcebergDestinationTestConfiguration
import pendulum
from pyiceberg.types import DoubleType, StringType
import pytest

from tests.e2e_tests.elt_common.dlt_destinations.pyiceberg.utils import (
    assert_table_has_shape,
    assert_table_has_data,
    partition_test_configs,
    sort_order_test_configs,
    PyIcebergPartitionTestConfiguration,
    PyIcebergSortOrderTestConfiguration,
)


def pipeline_name(frame: FrameType | None):
    # Use the function name
    return frame.f_code.co_name if frame is not None else "pipeline_name_frame_none"


def resource_factory(
    data: List[Dict[str, Any]] | None = None,
    primary_key: str | None = "id",
    merge_key: str | None = None,
):
    kwargs = {}
    if primary_key is not None:
        kwargs["primary_key"] = primary_key
    if merge_key is not None:
        kwargs["merge_key"] = merge_key

    if kwargs:
        decorator = dlt.resource(**kwargs)
    else:
        decorator = dlt.resource()

    @decorator
    def data_items():
        items = [] if data is None else data
        yield items

    return data_items


def test_dlt_tables_created(
    pipelines_dir: str,
    destination_config: PyIcebergDestinationTestConfiguration,
) -> None:
    data = [{"id": 1}]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline.run(resource_factory(data))

    assert_table_has_shape(
        pipeline,
        f"{pipeline.dataset_name}._dlt_loads",
        expected_row_count=1,
        expected_schema=loads_table(),
    )
    assert_table_has_shape(
        pipeline,
        f"{pipeline.dataset_name}._dlt_version",
        expected_row_count=1,
        expected_schema=version_table(),
    )
    assert_table_has_shape(
        pipeline,
        f"{pipeline.dataset_name}._dlt_pipeline_state",
        expected_row_count=1,
        expected_schema=pipeline_state_table(),
    )


def test_explicit_append(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
) -> None:
    data = [
        {
            "id": i + 1,
            "category": "A",
        }
        for i in range(10)
    ]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # first run
    pipeline.run(resource_factory(data), write_disposition="append")
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data),
        items=data,
    )

    # run again and see we get duplicated records
    data_second = [
        {
            "id": 11 + i,
            "category": "B",
        }
        for i in range(10)
    ]
    pipeline.run(resource_factory(data_second), write_disposition="append")
    final_data = data
    final_data.extend(data_second)
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(final_data),
        items=final_data,
    )


def test_explicit_replace(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
) -> None:
    data = [
        {
            "id": i + 1,
            "category": "A",
        }
        for i in range(10)
    ]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # first run
    pipeline.run(resource_factory(data))
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data),
        items=data,
    )

    # run again and see we a single copy of the records from the second dataset
    data_second = [
        {
            "id": 11 + i,
            "category": "B",
        }
        for i in range(10)
    ]
    pipeline.run(resource_factory(data_second), write_disposition="replace")
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data_second),
        items=data_second,
    )


@pytest.mark.parametrize(
    "identifier_keys",
    [
        {"primary_key": "id", "merge_key": None},
        {"primary_key": None, "merge_key": "id"},
    ],
)
def test_explicit_merge_updates_expected_values(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
    identifier_keys: dict,
) -> None:
    num_records_first_run = 1000
    data = [
        {
            "id": i + 1,
            "category": "A",
        }
        for i in range(num_records_first_run)
    ]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # first run
    pipeline.run(resource_factory(data, **identifier_keys))
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data),
        items=data,
    )

    id_updated_start, num_records_upserted = 501, 2000
    # run again and see we update existing records and added new ones
    data_updated = [
        {
            "id": id_updated_start + i,
            "category": "B",
        }
        for i in range(num_records_upserted)
    ]
    pipeline.run(resource_factory(data_updated, **identifier_keys), write_disposition="merge")
    expected_data = [
        {"id": i + 1, "category": "A" if i + 1 < id_updated_start else "B"}
        for i in range(num_records_first_run + num_records_upserted - id_updated_start + 1)
    ]
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(expected_data),
        items=expected_data,
    )


def test_merge_without_primary_or_merge_key_raises_error(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
) -> None:
    num_records_first_run = 1000
    data = [
        {
            "id": i + 1,
            "category": "A",
        }
        for i in range(num_records_first_run)
    ]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    with pytest.raises(Exception):
        pipeline.run(resource_factory(data, primary_key=None), write_disposition="merge")


@pytest.mark.parametrize("merge_strategy", ["delete-insert", "scd2"])
def test_explicit_merge_not_supported_for_strategies_other_than_upsert(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
    merge_strategy,
) -> None:
    data = [
        {
            "id": i + 1,
            "category": "A",
        }
        for i in range(10)
    ]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    with pytest.raises(Exception, match=f"`{merge_strategy}` merge strategy not supported"):
        pipeline.run(
            resource_factory(data),
            write_disposition={"disposition": "merge", "strategy": merge_strategy},
        )


def test_drop_storage(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
):
    data = [{"id": i + 1} for i in range(2)]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # run a pipeline to populate destination state and then drop the storage
    pipeline.run(resource_factory(data))

    with pipeline.destination_client() as client:
        client.drop_storage()

    with iceberg_catalog(pipeline) as catalog:
        assert not catalog_namespace_exists(catalog, pipeline.dataset_name)


def test_sync_state(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
):
    data = [{"id": i + 1} for i in range(2)]
    pipeline_1 = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # run a pipeline to populate destination state, remove local state and run again
    pipeline_1.run(resource_factory(data))
    shutil.rmtree(pipelines_dir)
    pipeline_2 = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline_2.run(resource_factory(data), write_disposition="replace")

    assert pipeline_2.state == pipeline_1.state


def test_expected_datatypes_can_be_loaded(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
):
    data = [
        {
            "integer": 1,
            "text": "text value",
            "boolean": True,
            "timestamp": pendulum.datetime(2025, 5, 7, 14, 29, 31),
            "date": pendulum.date(2025, 5, 7),
        }
    ]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline.run(resource_factory(data, primary_key=None))

    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data),
        items=data,
    )


def test_schema_evolution_supported(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
):
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    data_schema_1 = [{"id": 1}]
    pipeline.run(resource_factory(data_schema_1))
    data_schema_2 = [{"id": 2, "new_column": "string value"}]
    pipeline.run(resource_factory(data_schema_2))

    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=1 + len(data_schema_2),
        items=[{"id": 1, "new_column": None}] + data_schema_2,
    )


@pytest.mark.parametrize(
    "partition_config",
    partition_test_configs(),
    ids=lambda x: x.name,
)
def test_partition_specs_respected(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
    partition_config: PyIcebergPartitionTestConfiguration,
):
    data = partition_config.data

    @dlt.resource()
    def partitioned_data():
        yield data

    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    partitioned_resource = pyiceberg_adapter(
        partitioned_data, partition=partition_config.partition_request
    )

    pipeline.run(partitioned_resource)
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.partitioned_data",
        expected_items_count=len(data),
        items=data,
    )

    # check partitions
    with iceberg_catalog(pipeline) as catalog:
        table = catalog.load_table((pipeline.dataset_name, "partitioned_data"))
        assert table.spec() == partition_config.expected_spec


@pytest.mark.parametrize(
    "sort_order_config",
    sort_order_test_configs(),
    ids=lambda x: x.name,
)
def test_sort_order_specs_respected(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
    sort_order_config: PyIcebergSortOrderTestConfiguration,
):
    @dlt.resource()
    def sort_order_data():
        yield sort_order_config.data

    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    sorted_resource = pyiceberg_adapter(
        sort_order_data, sort_order=sort_order_config.sort_order_request
    )

    pipeline.run(sorted_resource)

    # check sort order
    with iceberg_catalog(pipeline) as catalog:
        table = catalog.load_table((pipeline.dataset_name, "sort_order_data"))
        assert table.sort_order() == sort_order_config.expected_spec


def test_evolve_schema_new_columns(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
):
    data_start = [{"id": 1, "summary": "Summary 1"}]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline.run(resource_factory(data_start))
    # data_items now has 4 columns (including the 2 _dlt columns added for lineage tracking)

    data_evolved = [
        {
            "id": 2,
            "summary": "Summary 2",
            "additional_comment": "Additional Comment 2",
            "double_value": 5.1,
        }
    ]
    pipeline.run(resource_factory(data_evolved))

    with iceberg_catalog(pipeline) as catalog:
        dest_table_evolved = catalog.load_table((pipeline.dataset_name, "data_items"))

    dest_schema_evolved = dest_table_evolved.schema()
    assert isinstance(dest_schema_evolved.find_field("additional_comment").field_type, StringType)
    assert isinstance(dest_schema_evolved.find_field("double_value").field_type, DoubleType)
    assert len(dest_schema_evolved.columns) == 6

    dest_scan = dest_table_evolved.scan()
    assert dest_scan.count() == 2

    dest_arrow = dest_scan.to_arrow()
    new_columns_data = dest_arrow.sort_by("id").select(("id", "additional_comment", "double_value"))
    assert new_columns_data.to_pylist() == [
        {"id": 1, "additional_comment": None, "double_value": None},
        {"id": 2, "additional_comment": "Additional Comment 2", "double_value": 5.1},
    ]


def test_evolve_schema_columns_removed(
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
):
    data_start = [{"id": 1, "summary": "Summary 1", "double_value": 2.0}]
    pipeline = destination_config.setup_pipeline(
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline.run(resource_factory(data_start))
    # data_items now has 5 columns (including the 2 _dlt columns added for lineage tracking)

    data_evolved = [{"id": 2, "summary": "Summary 2"}]
    pipeline.run(resource_factory(data_evolved))
    with iceberg_catalog(pipeline) as catalog:
        dest_table_evolved = catalog.load_table((pipeline.dataset_name, "data_items"))

    # schema maintains old column to keep old data
    dest_schema_evolved = dest_table_evolved.schema()
    assert len(dest_schema_evolved.columns) == 5

    dest_scan = dest_table_evolved.scan()
    assert dest_scan.count() == 2

    dest_arrow = dest_scan.to_arrow()
    columns_data = dest_arrow.sort_by("id").select(("id", "summary", "double_value"))
    assert columns_data.to_pylist() == [
        {"id": 1, "summary": "Summary 1", "double_value": 2.0},
        {"id": 2, "summary": "Summary 2", "double_value": None},
    ]
