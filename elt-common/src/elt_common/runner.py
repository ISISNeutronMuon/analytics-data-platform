"""Pipeline runner: orchestrates extract -> load -> transform for an elt job.

Each job is expected to define a class called Extract with the following structure. See
the example in tests/unit_tests/simple/simple.py for the expected structure.
"""

import importlib.util
import logging
import time
from typing import Any
from pathlib import Path

from elt_common.iceberg.catalog import connect_catalog, table_identifier
from elt_common.iceberg.io import IcebergIO
from elt_common.extract import BaseExtract, Watermark, WatermarkSerializerJSONMaxColumnValue
from elt_common.typing import ELTJobManifest

EXTRACT_CLS_NAME = "Extract"

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
    extract_obj = _create_extract_obj(job)
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
                iceberg_io.read_property(table_id, Watermark.property_key())
            )
        except KeyError:
            watermark_before_extract = None

        rows_seen.setdefault(table_name, 0)
        for data in table_props.extractor(watermark_before_extract):
            # Determine write mode. A replace is really a delete then append but each source table
            # can yield several times to load in chunks so only delete once and then continue appending
            if write_mode == "replace" and rows_seen[table_name] > 0:
                write_mode = "append"

            watermark_after_extract = (
                watermark_serializer.serialize(table_props.watermark_column, data)
                if table_props.watermark_column is not None
                else None
            )
            iceberg_io.write_table(
                table_id,
                data,
                write_mode,
                merge_on=merge_on,
                partition=partition_by,
                sort_order=sort_order,
                watermark=watermark_after_extract,
            )
            rows_seen[table_name] += data.num_rows

    return rows_seen


def _create_extract_obj(job: ELTJobManifest) -> BaseExtract:
    """Given the job directory and name create the object that will perform the data extraction"""
    extract_cls = _get_extract_cls(job)
    source_config_cls = extract_cls.source_config_cls

    return extract_cls(source_config_cls(_env_prefix=f"{job.name}__"))


def _get_extract_cls(job: ELTJobManifest) -> Any:
    """Get the class that will handle the extraction."""
    custom_extract_script = job.ingest_job_dir / f"{job.name}.py"
    if custom_extract_script.exists():
        return _get_extract_cls_from_module_path(job.name, custom_extract_script)
    else:
        raise RuntimeError(
            f"No extraction class definition file found at '{custom_extract_script}'"
        )


def _get_extract_cls_from_module_path(module_name: str, file_path: Path) -> Any:
    """Get the class attribute that will handle extraction

    :raises: AttributeError if the attribute doesn't exist
    """
    module = _import_module_from_path(module_name, file_path)
    return getattr(module, EXTRACT_CLS_NAME)


def _import_module_from_path(module_name: str, file_path: Path):
    """Import a module given its name and file location"""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None:
        raise ImportError(f"Unable to find module spec for '{module_name}' at '{file_path}'")
    module = importlib.util.module_from_spec(spec)
    if spec.loader is not None:
        spec.loader.exec_module(module)
    else:
        raise ImportError(f"Module spec for {module_name} @ '{file_path}' has no loader attribute")

    return module
