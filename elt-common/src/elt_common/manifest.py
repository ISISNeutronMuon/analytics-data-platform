"""Job manifest: reads and validates ``elt.toml`` files.

Each ingest job directory contains an ``elt.toml`` that declares the extract
entrypoint, table configurations (write mode, partitioning, merge keys), and
optional dbt transform settings.
"""

import tomllib
from pathlib import Path

from elt_common.typing import JobManifest, TransformProperties


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
