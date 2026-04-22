"""Pipeline runner: orchestrates extract -> load -> transform for an elt job.

Each job is expected to define a class called Extract with the following structure:

from elt_common.typing import (
    TableProperties,
    WatermarkInfo,
)
from pydantic import SecretStr
from pydantic_settings import BaseSettings

class MySourceConfig(BaseSettings):

    setting_1: str
    setting_2: str


class Extract:
    source_config_cls = MySourceConfig

    @abstractmethod
    def tables(self) -> dict[str, TableProperties]:
        # Define properties of tables to extract. See TableProperties

    def extract(self, watermarks: WatermarkInfo) -> Iterator[Tuple[str, pa.Table]]:
        # Yield a tuple of (table_name, pyarrow.Table) for each table
        # Can yield as many times as necessary to perform chunked loading
        yield table_name, pa.Table
"""

import importlib.util
import logging
import time
from typing import Any
from pathlib import Path

from elt_common.iceberg.catalog import connect_catalog
from elt_common.iceberg.io import IcebergIO
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


def run_ingest(job: ELTJobManifest) -> None:
    """Import the extract function, call it, and write results to Iceberg."""
    # Get object that will do the extraction. This step requires the appropriate
    # environment variables for that job to have been set before reaching here.
    extract_obj = _create_extract_obj(job)

    # Connect to catalog
    namespace = job.destination_namespace
    iceberg_io = IcebergIO(connect_catalog())
    iceberg_io.ensure_namespace(namespace)

    # Extract
    expected_tables = extract_obj.tables()
    tables_seen: dict[str, bool] = {}
    for table_name, data in extract_obj.extract():
        if table_name not in expected_tables:
            raise ValueError(
                f"Extract returned table '{table_name}' but it's not defined in [[tables]]"
            )
        if data.num_rows == 0:
            LOGGER.info(f"No data for table {table_name}, skipping.")
            continue

        # Determine write mode. A replace is really a delete then append but each source
        # table can yield several times so only delete once and then continue appending
        table_props = expected_tables[table_name]
        write_mode = table_props.write_mode
        if table_props.write_mode == "replace" and tables_seen.get(table_name, False):
            write_mode = "append"

        iceberg_io.write_table(
            table_name,
            data,
            mode=write_mode,
            merge_on=list(table_props.merge_on) if table_props.merge_on else None,
            partition=table_props.partition or None,
            sort_order=table_props.sort_order or None,
        )
        tables_seen[table_name] = True


# "private" helpers


def _create_extract_obj(job: ELTJobManifest):
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
