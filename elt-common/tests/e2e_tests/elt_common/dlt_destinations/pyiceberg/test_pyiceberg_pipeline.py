import inspect
import shutil
from types import FrameType
from typing import Any, Dict, List

import dlt
from dlt.common.pendulum import pendulum
from dlt.common.schema.utils import loads_table, pipeline_state_table, version_table
from dlt.pipeline.exceptions import PipelineStepFailed

import pytest

from elt_common.dlt_destinations.pyiceberg.catalog import (
    namespace_exists as catalog_namespace_exists,
)
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
)

from e2e_tests.conftest import Warehouse
from e2e_tests.elt_common.dlt_destinations.pyiceberg.utils import (
    assert_table_has_shape,
    assert_table_has_data,
    iceberg_catalog,
    partition_test_configs,
    sort_order_test_configs,
    PyIcebergDestinationTestConfiguration,
    PyIcebergPartitionTestConfiguration,
    PyIcebergSortOrderTestConfiguration,
)


def pipeline_name(frame: FrameType | None):
    # Use the function name
    return frame.f_code.co_name if frame is not None else "pipeline_name_frame_none"


@dlt.resource()
def data_items(data: List[Dict[str, Any]] | None = None):
    data = [] if data is None else data
    yield data


def test_dlt_tables_created(
    warehouse: Warehouse,
    pipelines_dir: str,
    destination_config: PyIcebergDestinationTestConfiguration,
) -> None:
    data = [{"id": 1}]
    pipeline = destination_config.setup_pipeline(
        warehouse,
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline.run(data_items(data))

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
    warehouse: Warehouse,
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
        warehouse,
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # first run
    pipeline.run(data_items(data), write_disposition="append")
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
    pipeline.run(data_items(data_second), write_disposition="append")
    final_data = data
    final_data.extend(data_second)
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(final_data),
        items=final_data,
    )


def test_explicit_replace(
    warehouse: Warehouse,
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
        warehouse,
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # first run
    pipeline.run(data_items(data))
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
    pipeline.run(data_items(data_second), write_disposition="replace")
    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data_second),
        items=data_second,
    )


def test_drop_storage(
    warehouse: Warehouse,
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
):
    data = [{"id": i + 1} for i in range(2)]
    pipeline = destination_config.setup_pipeline(
        warehouse,
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # run a pipeline to populate destination state and then drop the storage
    pipeline.run(data_items(data))

    with pipeline.destination_client() as client:
        client.drop_storage()

    with iceberg_catalog(pipeline) as catalog:
        assert not catalog_namespace_exists(catalog, pipeline.dataset_name)


def test_sync_state(
    warehouse: Warehouse,
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
):
    data = [{"id": i + 1} for i in range(2)]
    pipeline_1 = destination_config.setup_pipeline(
        warehouse,
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    # run a pipeline to populate destination state, remove local state and run again
    pipeline_1.run(data_items(data))
    shutil.rmtree(pipelines_dir)
    pipeline_2 = destination_config.setup_pipeline(
        warehouse,
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline_2.run(data_items(data), write_disposition="replace")

    assert pipeline_2.state == pipeline_1.state


def test_expected_datatypes_can_be_loaded(
    warehouse: Warehouse,
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
        warehouse,
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    pipeline.run(data_items(data))

    assert_table_has_data(
        pipeline,
        f"{pipeline.dataset_name}.data_items",
        expected_items_count=len(data),
        items=data,
    )


def test_schema_evolution_not_supported(
    warehouse: Warehouse,
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
):
    pipeline = destination_config.setup_pipeline(
        warehouse,
        pipeline_name(inspect.currentframe()),
        pipelines_dir=pipelines_dir,
    )
    data_schema_1 = [{"id": 1}]
    pipeline.run(data_items(data_schema_1))
    data_schema_2 = [{"id": 2, "new_column": "string value"}]

    with pytest.raises(PipelineStepFailed):
        pipeline.run(data_items(data_schema_2))


@pytest.mark.parametrize(
    "partition_config",
    partition_test_configs(),
    ids=lambda x: x.name,
)
def test_partition_specs_respected(
    warehouse: Warehouse,
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
    partition_config: PyIcebergPartitionTestConfiguration,
):
    data = partition_config.data

    @dlt.resource()
    def partitioned_data():
        yield data

    pipeline = destination_config.setup_pipeline(
        warehouse,
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
    warehouse: Warehouse,
    pipelines_dir,
    destination_config: PyIcebergDestinationTestConfiguration,
    sort_order_config: PyIcebergSortOrderTestConfiguration,
):
    @dlt.resource()
    def sort_order_data():
        yield sort_order_config.data

    pipeline = destination_config.setup_pipeline(
        warehouse,
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
