"""Job manifest: reads and validates ``elt.toml`` files.

Each ingest job directory contains an ``elt.toml`` that declares the extract
entrypoint, table configurations (write mode, partitioning, merge keys), and
optional dbt transform settings.
"""

import tomllib
from pathlib import Path

from elt_common.typing import JobManifest, TableCursor, TableProperties, TransformProperties


def load_manifest(job_dir: Path) -> JobManifest:
    """Read and validate an ``elt.toml`` file from the given directory.

    :param job_dir: Path to the directory containing ``elt.toml``.
    :raises FileNotFoundError: If ``elt.toml`` does not exist.
    :raises ValueError: If required fields are missing.
    """
    manifest_path = job_dir / "elt.toml"
    with open(manifest_path, "rb") as f:
        raw = tomllib.load(f)

    job = raw.pop("job", {})
    for field in ("name", "domain", "warehouse"):
        if field not in job:
            raise ValueError(f"Missing required field 'job.{field}' in {manifest_path}")

    tables = {}
    for table_raw in raw.pop("tables", []):
        if "name" not in table_raw:
            raise ValueError(f"Missing required field 'tables.name' in {manifest_path}")
        name = table_raw["name"]
        tables[name] = TableProperties(
            name=table_raw.pop("name"),
            write_mode=table_raw.pop("write_mode", "append"),
            cursor_column=TableCursor(table_raw.pop("cursor_column"), None),
            merge_on=tuple(table_raw.pop("merge_on", ())),
            partition=table_raw.pop("partition", {}),
            sort_order=table_raw.pop("sort_order", {}),
        )
        if len(table_raw) > 0:
            raise ValueError(
                f"Found unknown keys in [[tables]] section of manifest '{manifest_path}': {table_raw}."
            )

    transform = None
    if "transform" in raw:
        tr = raw["transform"]
        transform = TransformProperties(
            dbt_dir=tr.get("dbt_dir", ""),
            dbt_select=tr.get("dbt_select", ""),
        )

    manifest = JobManifest(
        name=job["name"],
        domain=job["domain"],
        warehouse=job["warehouse"],
        tables=tables,
        transform=transform,
        job_dir=job_dir.resolve(),
    )
    return manifest


def discover_jobs(root: Path) -> list[JobManifest]:
    """Find all ``elt.toml`` files under *root* and parse them.

    :param root: Root directory to search recursively.
    :returns: List of parsed manifests, sorted by name.
    """
    manifests = []
    for toml_path in sorted(root.rglob("elt.toml")):
        manifests.append(load_manifest(toml_path.parent))
    return manifests
