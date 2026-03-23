"""Pipeline runner: orchestrates extract → load → transform for an elt job."""

import importlib.util
import logging
import subprocess
import time
from typing import Any
from pathlib import Path

from elt_common.iceberg.catalog import CatalogConfig, get_max_value
from elt_common.iceberg.writer import IcebergWriter
from elt_common.manifest import JobManifest, load_manifest

EXTRACT_CLS_NAME = "Extract"

logger = logging.getLogger(__name__)


def namespace_name(source_domain: str, job_name: str) -> str:
    """Given a domain and job name, construct a namespace name."""
    return f"{source_domain}_{job_name}"


def import_module_from_path(module_name: str, file_path: Path):
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


def get_extract_cls_from_module_path(module_name: str, file_path: Path) -> Any:
    """Get the class attribute that will handle extraction

    :raises: AttributeError if the attribute doesn't exist
    """
    module = import_module_from_path(module_name, file_path)
    return getattr(module, EXTRACT_CLS_NAME)


def run_job(
    job_dir: Path,
    *,
    steps: str = "all",
) -> None:
    """Run an ELT job defined by the ``elt.toml`` in *job_dir*.

    :param job_dir: Directory containing ``elt.toml``.
    :param steps: ``"all"``, ``"ingest"``, or ``"transform"``.
    :param backfill: If ``True``, passed to the extract function via config.
    """
    manifest = load_manifest(job_dir)
    namespace = namespace_name(manifest.domain, manifest.name)
    logger.info(f"Starting job: {manifest.name} (namespace={namespace})")
    t0 = time.monotonic()

    if steps in ("all", "ingest"):
        _run_ingest(namespace, manifest)

    # if steps in ("all", "transform"):
    #     _run_transform(manifest)

    elapsed = time.monotonic() - t0
    logger.info(f"Job {manifest.name} completed in {elapsed:.1f}s")


def _run_ingest(namespace: str, manifest: JobManifest) -> None:
    """Import the extract function, call it, and write results to Iceberg."""
    catalog_config = CatalogConfig()
    catalog = catalog_config.connect_catalog()

    writer = IcebergWriter(catalog, namespace)
    writer.ensure_namespace()

    extract_cls = get_extract_cls_from_module_path(
        manifest.name, manifest.job_dir / f"{manifest.name}.py"
    )
    source_config = extract_cls.source_config_cls(_env_prefix=f"{manifest.name}__")
    extract_obj = extract_cls(source_config)

    expected_tables = manifest.tables
    for _, t in expected_tables.items():
        if t.cursor_column is not None:
            t.cursor_column.max_value = get_max_value(
                catalog, writer.table_id(t.name), t.cursor_column.column
            )
    tables_seen: dict[str, bool] = {}
    for table_name, data in extract_obj(manifest.tables):
        if table_name not in expected_tables:
            raise ValueError(
                f"Extract returned table '{table_name}' but it's not defined in [[tables]]"
            )
        if data.num_rows == 0:
            logger.info(f"No data for table {table_name}, skipping.")
            continue

        # Determine write mode. A replace is really a delete then append but each source
        # table can yield several times so only delete once and then continue appending
        table_props = expected_tables[table_name]
        write_mode = table_props.write_mode
        if table_props.write_mode == "replace" and tables_seen.get(table_name, False):
            write_mode = "append"

        writer.write_table(
            table_name,
            data,
            mode=write_mode,
            merge_on=list(table_props.merge_on) if table_props.merge_on else None,
            partition=table_props.partition or None,
            sort_order=table_props.sort_order or None,
        )
        tables_seen[table_name] = True


def _run_transform(manifest: JobManifest) -> None:
    """Run the dbt transform step if configured."""
    if manifest.transform is None:
        logger.debug("No [transform] section, skipping dbt.")
        return

    dbt_dir = manifest.transform.dbt_dir
    dbt_select = manifest.transform.dbt_select
    if not dbt_dir:
        logger.debug("No dbt_dir configured, skipping transform.")
        return

    cmd = ["dbt", "run"]
    if dbt_select:
        cmd.extend(["--select", dbt_select])

    logger.info(f"Running dbt: {' '.join(cmd)} (cwd={dbt_dir})")
    subprocess.run(cmd, cwd=dbt_dir, check=True)
