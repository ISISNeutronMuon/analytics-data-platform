"""Job manifest: reads and validates ``elt.toml`` files.

Each ingest job directory contains an ``elt.toml`` that declares the extract
entrypoint, table configurations (write mode, partitioning, merge keys), and
optional dbt transform settings.
"""

import dataclasses
import tomllib
from pathlib import Path
from typing import Sequence

from elt_common.types import WriteMode


@dataclasses.dataclass(frozen=True)
class TableConfig:
    """Configuration for a single Iceberg table within a job."""

    name: str
    write_mode: WriteMode = "append"
    cursor_column: str | None = None
    merge_on: Sequence[str] = ()
    partition: dict[str, str] = dataclasses.field(default_factory=dict)
    sort_order: dict[str, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class TransformConfig:
    """Optional dbt transform configuration."""

    dbt_dir: str
    dbt_select: str = ""


@dataclasses.dataclass(frozen=True)
class JobManifest:
    """Parsed representation of an ``elt.toml`` file."""

    name: str
    domain: str
    warehouse: str
    tables: Sequence[TableConfig]
    transform: TransformConfig | None = None
    job_dir: Path = dataclasses.field(default=Path("."))

    @property
    def namespace(self) -> str:
        """The Iceberg namespace for this job: ``{domain}_{name}``."""
        return f"{self.domain}_{self.name}"


def load_manifest(job_dir: Path) -> JobManifest:
    """Read and validate an ``elt.toml`` file from the given directory.

    :param job_dir: Path to the directory containing ``elt.toml``.
    :raises FileNotFoundError: If ``elt.toml`` does not exist.
    :raises ValueError: If required fields are missing.
    """
    manifest_path = job_dir / "elt.toml"
    with open(manifest_path, "rb") as f:
        raw = tomllib.load(f)

    job = raw.get("job", {})
    for field in ("name", "domain", "warehouse"):
        if field not in job:
            raise ValueError(f"Missing required field 'job.{field}' in {manifest_path}")

    tables = []
    for table_raw in raw.get("tables", []):
        if "name" not in table_raw:
            raise ValueError(f"Missing required field 'tables.name' in {manifest_path}")
        tables.append(
            TableConfig(
                name=table_raw["name"],
                write_mode=table_raw.get("write_mode", "append"),
                cursor_column=table_raw.get("cursor_column", None),
                merge_on=tuple(table_raw.get("merge_on", ())),
                partition=table_raw.get("partition", {}),
                sort_order=table_raw.get("sort_order", {}),
            )
        )

    transform = None
    if "transform" in raw:
        tr = raw["transform"]
        transform = TransformConfig(
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
