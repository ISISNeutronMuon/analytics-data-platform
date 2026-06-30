"""Tests for elt_common.pipeline"""

from pathlib import Path

import pytest

from elt_common.pipeline import (
    ELTJobManifest,
    PipelinesProject,
)


def test_properties():
    manifest = ELTJobManifest(
        warehouse_name="test_warehouse",
        name="source_a",
        domain="facility_ops",
        is_ingest_job=True,
        ingest_job_dir=Path("/some/path"),
    )
    assert manifest.destination_namespace == "facility_ops_source_a", (
        "Destination namespace should be 'domain'_'name'"
    )
    assert manifest.full_name == "facility_ops.source_a", "Name should be 'domain'.'name'"
    assert manifest.destination_warehouse == "test_warehouse_landing", (
        "Ingest job destination should have _landing appended"
    )

    non_ingest_manifest = ELTJobManifest(
        warehouse_name="test_warehouse",
        name="source_a",
        domain="facility_ops",
        is_ingest_job=False,
        ingest_job_dir=Path("/some/path"),
    )
    assert non_ingest_manifest.destination_warehouse == "test_warehouse", (
        "Non-ingest manifest shouldn't change warehouse name"
    )


def test_init_stores_root_and_derives_name(tmp_path: Path):
    """Test that PipelinesProject stores root and derives name from it"""
    root = tmp_path / "my_project"
    root.mkdir()
    (root / "ingest").mkdir()

    project = PipelinesProject(root)

    assert project.name == "my_project"


def test_ingest_jobs_property_discovers_jobs_lazily(tmp_path: Path):
    """Test that ingest_jobs property discovers jobs on first access"""
    root = tmp_path / "my_project"
    (root / "ingest" / "domain_a" / "source_1").mkdir(parents=True)
    (root / "ingest" / "domain_a" / "source_2").mkdir(parents=True)
    (root / "ingest" / "domain_b" / "source_a").mkdir(parents=True)
    (root / "transform").mkdir(parents=True)

    project = PipelinesProject(root)
    jobs = project.ingest_jobs

    assert len(jobs) == 3
    assert all(isinstance(job, ELTJobManifest) for job in jobs)


def test_ingest_jobs_caches_results(tmp_path: Path):
    """Test that ingest_jobs property caches the discovered jobs"""
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
