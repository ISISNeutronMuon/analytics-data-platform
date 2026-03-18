"""Pipeline runner: orchestrates extract → load → transform for an elt job."""

import importlib.util
import logging
import subprocess
import sys
import time
from pathlib import Path

import pyarrow as pa
from elt_common.catalog import CatalogConfig
from elt_common.iceberg.writer import IcebergWriter
from elt_common.manifest import JobManifest, load_manifest

logger = logging.getLogger(__name__)


def dataset_name(source_domain: str, pipeline_name: str) -> str:
    """Given a domain and pipeline name, construct a dataset name.

    Retained for backward compatibility with existing scripts.
    """
    return f"{source_domain}_{pipeline_name}"


def import_module_from_path(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_job(
    job_dir: Path,
    *,
    steps: str = "all",
    backfill: bool = False,
) -> None:
    """Run an ELT job defined by the ``elt.toml`` in *job_dir*.

    :param job_dir: Directory containing ``elt.toml``.
    :param steps: ``"all"``, ``"ingest"``, or ``"transform"``.
    :param backfill: If ``True``, passed to the extract function via config.
    """
    manifest = load_manifest(job_dir)
    namespace = f"{manifest.domain}_{manifest.name}"
    logger.info(f"Starting job: {manifest.name} (namespace={namespace})")
    t0 = time.monotonic()

    if steps in ("all", "ingest"):
        _run_ingest(namespace, manifest, backfill=backfill)

    # if steps in ("all", "transform"):
    #     _run_transform(manifest)

    elapsed = time.monotonic() - t0
    logger.info(f"Job {manifest.name} completed in {elapsed:.1f}s")


def _run_ingest(namespace: str, manifest: JobManifest, *, backfill: bool) -> None:
    """Import the extract function, call it, and write results to Iceberg."""
    catalog_config = CatalogConfig()  # type: ignore[call-arg]
    catalog = catalog_config.connect_catalog()

    writer = IcebergWriter(catalog, namespace)
    writer.ensure_namespace()

    # Import the source config and extract function
    module_name = manifest.name
    module = import_module_from_path(module_name, manifest.job_dir / f"{module_name}.py")
    source_config_cls = getattr(module, manifest.extract_sourceconfigcls)
    extract_fn = getattr(module, manifest.extract_entrypoint)

    source_config = source_config_cls()
    for table_name, data in extract_fn(source_config, backfill=backfill):
        # Build a lookup of table configs by name
        table_configs = {t.name: t for t in manifest.tables}

        if data.num_rows == 0:
            logger.info(f"No data for table {table_name}, skipping.")
            continue

        tc = table_configs.get(table_name)
        if tc is None:
            raise ValueError(
                f"Extract returned table '{table_name}' but it's not defined in [[tables]]"
            )

        writer.write_table(
            table_name,
            data,
            mode=tc.write_mode,
            merge_on=list(tc.merge_on) if tc.merge_on else None,
            partition=tc.partition or None,
            sort_order=tc.sort_order or None,
        )


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
