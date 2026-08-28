"""Utilities for capturing and describing information about ELT jobs in a set of elt pipelines."""

from pathlib import Path

from .pipeline_types import ELTIngestManifest

INGEST = "ingest"
TRANSFORM = "transform"


class PipelinesProject:
    """Captures a set of elt pipelines based at a given root directory

    Two types of 'pipeline' can be included in a project:
    - 'ingest' pipelines, which can be run by the ingest module to extract
    and load data into iceberg
    - 'transform' pipelines, which are models in a dbt project

    ingest pipelines are required for the project to be valid. transforms are
    optional, but an error will be thrown if a nonexistent transform directory
    is used.
    """

    def __init__(self, root: Path) -> None:
        ingest_dir = root / INGEST
        if not ingest_dir.is_dir():
            raise ValueError(f"Invalid project. Ingest directory '{ingest_dir}' does not exist.")

        resolved = root.resolve()
        self._root = resolved
        self._warehouse = resolved.parts[-1]
        self._ingest_dir = ingest_dir
        self._transform_dir = root / TRANSFORM
        self._name = root.name
        self._ingest_jobs = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def ingest_dir(self) -> Path:
        return self._ingest_dir

    @property
    def transform_dir(self):
        if not self._transform_dir.is_dir():
            raise ValueError("Project doesn't have a transform directory")
        return self._transform_dir

    @property
    def ingest_jobs(self) -> list[ELTIngestManifest]:
        if not self._ingest_jobs:
            self._ingest_jobs = _discover_jobs(self._warehouse, self._ingest_dir)

        return self._ingest_jobs


def _discover_jobs(warehouse_name: str, ingest_dir: Path):
    """Find all subdirectories under the warehouse 'ingest' directory and create manifests describing them.

    The following directory structure is assumed:

    <warehouse_name>/
    |-- ingest/
    |   |-- domain_A/
    |   |   |-- source_A/
    |   |   |-- source_B/
    |   |-- domain_B/
    |       |-- source_A/

    Each subdirectory under ingest is considered a domain and each subdirectory
    underneath a domain is a data source from that domain.

    :param warehouse_name: The top level directory name, which is stored in the manifest.
    :param ingest_dir: Root ingest directory to search for jobs.
    :returns: List of parsed manifests.
    """

    return [
        _create_ingest_manifest(warehouse_name, job_dir)
        for domain_dir in ingest_dir.iterdir()
        if domain_dir.is_dir()
        for job_dir in domain_dir.iterdir()
        if job_dir.is_dir()
    ]


def _create_ingest_manifest(warehouse_name: str, job_dir: Path) -> ELTIngestManifest:
    return ELTIngestManifest(
        warehouse_name=warehouse_name,
        name=job_dir.name,
        domain=job_dir.parent.name,
        job_dir=job_dir.resolve(),
    )
