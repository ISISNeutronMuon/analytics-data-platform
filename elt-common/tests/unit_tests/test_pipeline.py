"""Tests for elt_common.pipeline"""

from pathlib import Path

from elt_common.pipeline import (
    ELTJobManifest,
    PipelinesProject,
)
import pytest


def test_namespace_property_combines_domain_and_name():
    """Tests for IngestJobManifest.destination_namespace"""
    manifest = ELTJobManifest(
        name="source_a",
        domain="facility_ops",
        ingest_job_dir=Path("/some/path"),
    )
    assert manifest.destination_namespace == "facility_ops_source_a"


def test_full_name_property_combines_domain_and_name():
    """Tests for IngestJobManifest.full_name"""
    manifest = ELTJobManifest(
        name="source_a",
        domain="facility_ops",
        ingest_job_dir=Path("/some/path"),
    )
    assert manifest.full_name == "facility_ops.source_a"


def test_init_stores_root_and_derives_name(tmp_path: Path):
    """Test that PipelinesProject stores root and derives name from it"""
    root = tmp_path / "my_project"
    root.mkdir()
    (root / "ingest").mkdir()

    project = PipelinesProject(root)

    assert project.name == "my_project"


def test_injest_jobs_property_discovers_jobs_lazily(tmp_path: Path):
    """Test that injest_jobs property discovers jobs on first access"""
    root = tmp_path / "my_project"
    (root / "ingest" / "domain_a" / "source_1").mkdir(parents=True)
    (root / "ingest" / "domain_a" / "source_2").mkdir(parents=True)
    (root / "ingest" / "domain_b" / "source_a").mkdir(parents=True)
    (root / "transform").mkdir(parents=True)

    project = PipelinesProject(root)
    jobs = project.ingest_jobs

    assert len(jobs) == 3
    assert all(isinstance(job, ELTJobManifest) for job in jobs)


def test_injest_jobs_caches_results(tmp_path: Path):
    """Test that injest_jobs property caches the discovered jobs"""
    root = tmp_path / "my_project"
    (root / "ingest" / "domain_a" / "source_1").mkdir(parents=True)

    project = PipelinesProject(root)
    jobs1 = project.ingest_jobs
    jobs2 = project.ingest_jobs

    assert jobs1 is jobs2  # Should be the same object (cached)


def test_init_without_ingest_directory_raises_error(tmp_path: Path):
    """Test that PipelinesProject raises error without ingest directory"""
    root = tmp_path / "my_project"

    with pytest.raises(ValueError, match="Invalid project"):
        PipelinesProject(root)
