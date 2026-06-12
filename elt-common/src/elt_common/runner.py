"""Pipeline runner: orchestrates extract -> load for an elt job.

To be run, a job must define a class called Extract which extends :py:class:`elt_common.extract.BaseExtract`.
"""

import datetime as dt
import logging
import time
from collections import defaultdict

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.exceptions import NoSuchTableError

from elt_common.extract import (
    ResourceProperties,
    Watermark,
    create_extract_obj,
)
from elt_common.iceberg.catalog import connect_catalog, table_identifier
from elt_common.iceberg.io import IcebergIO
from elt_common.typing import ELTJobManifest, WriteMode

INGEST_PROPERTY_KEY_LAST_UPDATED_AT = "ingest.last_updated_at"
INGEST_PROPERTY_KEY_WATERMARK = "ingest.watermark"

LOGGER = logging.getLogger(__name__)


def run_job(job: ELTJobManifest) -> None:
    """Run an ELT job defined by the given manifest."""
    LOGGER.info(f"Starting job: {job.full_name}")

    t0 = time.monotonic()
    run_ingest(job)
    elapsed = time.monotonic() - t0

    LOGGER.info(f"Job {job.full_name} completed in {elapsed:.1f}s")


def run_ingest(job: ELTJobManifest) -> dict[str, int]:
    """Import the extract function, call it, and write results to Iceberg."""

    iceberg_io = IcebergIO(connect_catalog())

    # Get object that will do the extraction.
    # Environment variables for the object's configuration must have been set
    # before reaching here.
    extract_obj = create_extract_obj(job)

    namespace = job.destination_namespace
    iceberg_io.ensure_namespace(namespace)

    rows_seen: dict[str, int] = defaultdict(int)  # track number of rows
    for table_name, table_props in extract_obj.extract_resource_properties():
        LOGGER.info(f"Extracting '{table_name}'")

        table_id = table_identifier(namespace, table_name)
        write_props = table_props.write_properties
        merge_on, partition_by, sort_order = (
            write_props.merge_on,
            write_props.partition,
            write_props.sort_order,
        )
        write_mode: WriteMode = write_props.write_mode

        try:
            watermark_before_extract = Watermark.deserialize(
                iceberg_io.read_property(table_id, INGEST_PROPERTY_KEY_WATERMARK)
            )
            LOGGER.info(
                f"Watermark: {watermark_before_extract.column} > {watermark_before_extract.value}"
            )
        except (NoSuchTableError, KeyError):
            watermark_before_extract = None

        for data in table_props.extractor(watermark_before_extract):
            # 'replace' really means delete the contents of the table, then append the new data.
            # Extractors can return multiple chunks of data, in which case only the first chunk
            # should cause a deletion, whilst the remaining chunks should be appended.
            if write_mode == "replace" and rows_seen[table_name] > 0:
                write_mode = "append"

            iceberg_io.write_table(
                table_id,
                data,
                write_mode,
                merge_on=merge_on,
                partition=partition_by,
                sort_order=sort_order,
                properties=_create_ingest_properties(data, table_props),
            )
            rows_seen[table_name] += data.num_rows

    return rows_seen


def _create_ingest_properties(
    data: pa.Table,
    resource_properties: ResourceProperties,
) -> dict[str, str]:
    """Create a set of properties describing the ingestion."""
    ingest_props = {
        INGEST_PROPERTY_KEY_LAST_UPDATED_AT: dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    }

    if resource_properties.watermark_column is not None:
        watermark_value = pc.max(data[resource_properties.watermark_column]).as_py()
        watermark = Watermark(resource_properties.watermark_column, watermark_value)
        ingest_props[INGEST_PROPERTY_KEY_WATERMARK] = watermark.serialize()

    return ingest_props
