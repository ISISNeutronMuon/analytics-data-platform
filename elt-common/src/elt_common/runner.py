"""Pipeline runner: orchestrates extract → load → transform for an elt job."""

import importlib
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
    logger.info(f"Starting job: {manifest.name} (namespace={manifest.namespace})")
    t0 = time.monotonic()

    if steps in ("all", "ingest"):
        _run_ingest(manifest, backfill=backfill)

    if steps in ("all", "transform"):
        _run_transform(manifest)

    elapsed = time.monotonic() - t0
    logger.info(f"Job {manifest.name} completed in {elapsed:.1f}s")


def _run_ingest(manifest: JobManifest, *, backfill: bool) -> None:
    """Import the extract function, call it, and write results to Iceberg."""
    catalog_config = CatalogConfig()  # type: ignore[call-arg]
    catalog = catalog_config.connect_catalog()

    writer = IcebergWriter(catalog, manifest.namespace)
    writer.ensure_namespace()

    # Import the extract function
    module_name, func_name = manifest.extract_entrypoint.split(":")
    sys.path.insert(0, str(manifest.job_dir))
    try:
        mod = importlib.import_module(module_name)
    finally:
        sys.path.pop(0)
    extract_fn = getattr(mod, func_name)

    # Build source config and call extract
    source_config = dict(manifest.source_config)
    source_config["backfill"] = backfill
    result = extract_fn(source_config, catalog)

    # The extract function can return:
    #  - dict[str, pa.Table]: multiple tables
    #  - pa.Table: a single table (matched to the first table in the manifest)
    if isinstance(result, pa.Table):
        if not manifest.tables:
            raise ValueError(
                f"Extract returned a single table but no [[tables]] defined in elt.toml"
            )
        tables = {manifest.tables[0].name: result}
    elif isinstance(result, dict):
        tables = result
    else:
        raise TypeError(
            f"Extract function must return pa.Table or dict[str, pa.Table], got {type(result)}"
        )

    # Build a lookup of table configs by name
    table_configs = {t.name: t for t in manifest.tables}

    for table_name, data in tables.items():
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
