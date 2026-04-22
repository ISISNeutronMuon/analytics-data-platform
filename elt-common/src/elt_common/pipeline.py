"""Utilities for capturing and describing information about ELT jobs in a set of elt pipelines."""

from pathlib import Path

from .runner import run_job
from .typing import ELTJobManifest

INGEST = "ingest"


class PipelinesProject:
    """Captures a set of elt pipelines based at a given root directory"""

    def __init__(self, root: Path) -> None:
        ingest_dir = root / INGEST
        if not ingest_dir.is_dir():
            raise ValueError(f"Invalid project. Ingest directory '{ingest_dir}' does not exist.")

        self._root = root
        self._ingest_dir = ingest_dir
        self._name = root.name
        self._ingest_jobs = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def ingest_dir(self) -> Path:
        return self._ingest_dir

    @property
    def ingest_jobs(self) -> list[ELTJobManifest]:
        if not self._ingest_jobs:
            self._ingest_jobs = _discover_jobs(self._ingest_dir)

        return self._ingest_jobs

    def run_job(self, name: str):
        """Run a named job.

        The name is assumed to be a qualified name, e.g domain.job"""
        domain, job_name = name.split(".")
        run_job(_create_ingest_manifest(self.ingest_dir / domain / job_name))


def _discover_jobs(ingest_dir: Path):
    """Find all subdirectories under *root/ingest* and create manifests describing them.

    The following directory structure is assumed:

    root/
    |-- ingest/
    |   |-- domain_A/
    |   |   |-- source_A/
    |   |   |-- source_B/
    |   |-- domain_B/
    |       |-- source_A/
    |-- transform/   # Root of dbt project

    Each subdirectory under ingest is considered a domain and each subdirectory
    underneath a domain is a data source from that domain.

    :param root: Root directory to search recursively.
    :returns: List of parsed manifests, sorted by name.
    """

    return [
        _create_ingest_manifest(job_dir)
        for domain_dir in ingest_dir.iterdir()
        if domain_dir.is_dir()
        for job_dir in domain_dir.iterdir()
        if job_dir.is_dir()
    ]


def _create_ingest_manifest(job_dir: Path) -> ELTJobManifest:
    return ELTJobManifest(
        name=job_dir.name,
        domain=job_dir.parent.name,
        ingest_job_dir=job_dir.resolve(),
    )
