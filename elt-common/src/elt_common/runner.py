"""Pipeline runner: orchestrates extract -> load -> transform for an elt job.

Each job is expected to define a class called Extract with the following structure. See
the example in tests/unit_tests/simple/simple.py for the expected structure.
"""

import logging
import time

from elt_common.iceberg.catalog import connect_catalog, table_identifier
from elt_common.iceberg.io import IcebergIO
from elt_common.extract import (
    ResourceProperties,
    WatermarkSerializer,
    WatermarkSerializerJSONMaxColumnValue,
    create_extract_obj,
)
from elt_common.typing import ELTJobManifest
import pyarrow as pa

INGEST_PROPERTY_WATERMARK_KEY = "ingest.watermark"

LOGGER = logging.getLogger(__name__)


def run_job(job: ELTJobManifest) -> None:
    """Run an ELT job defined by the given manifest."""
    LOGGER.info(f"Starting job: {job.full_name})")

    t0 = time.monotonic()
    run_ingest(job)
    elapsed = time.monotonic() - t0

    LOGGER.info(f"Job {job.full_name} completed in {elapsed:.1f}s")


def run_ingest(job: ELTJobManifest) -> dict[str, int]:
    """Import the extract function, call it, and write results to Iceberg."""
    # Connect to catalog
    iceberg_io = IcebergIO(connect_catalog())

    # Extract
    # Get object that will do the extraction. This step requires the appropriate
    # environment variables for that job to have been set before reaching here.
    extract_obj = create_extract_obj(job)
    watermark_serializer = WatermarkSerializerJSONMaxColumnValue()

    namespace = job.destination_namespace
    iceberg_io.ensure_namespace(namespace)
    expected_tables = extract_obj.resource_properties()
    rows_seen: dict[str, int] = {}  # track number of rows
    for table_name, table_props in expected_tables.items():
        table_id = table_identifier(namespace, table_name)
        partition_by, sort_order, merge_on, write_mode = (
            table_props.partition,
            table_props.sort_order,
            table_props.merge_on,
            table_props.write_mode,
        )
        try:
            watermark_before_extract = watermark_serializer.deserialize(
                iceberg_io.read_property(table_id, INGEST_PROPERTY_WATERMARK_KEY)
            )
        except KeyError:
            watermark_before_extract = None

        rows_seen.setdefault(table_name, 0)
        for data in table_props.extractor(watermark_before_extract):
            # Determine write mode. A replace is really a delete then append but each source table
            # can yield several times to load in chunks so only delete once and then continue appending
            if write_mode == "replace" and rows_seen[table_name] > 0:
                write_mode = "append"

            iceberg_io.write_table(
                table_id,
                data,
                write_mode,
                merge_on=merge_on,
                partition=partition_by,
                sort_order=sort_order,
                properties=_create_ingest_properties(data, table_props, watermark_serializer),
            )
            rows_seen[table_name] += data.num_rows

    return rows_seen


## private


def _create_ingest_properties(
    data: pa.Table,
    resource_properties: ResourceProperties,
    watermark_serializer: WatermarkSerializer,
) -> dict[str, str]:
    """Create a set of properties describing the ingestion"""
    ingest_props = {}

    if resource_properties.watermark_column is not None:
        ingest_props[INGEST_PROPERTY_WATERMARK_KEY] = watermark_serializer.serialize(
            resource_properties.watermark_column, data
        )

    return ingest_props
