"""Tests for the elt CLI"""

from pathlib import Path

from click.testing import CliRunner
from elt_common.cli import cli
import pytest


@pytest.fixture()
def root_dir(tmp_path: Path) -> Path:
    root = tmp_path / "my_project"
    (root / "ingest" / "domain_a" / "source_1").mkdir(parents=True)
    (root / "ingest" / "domain_a" / "source_2").mkdir(parents=True)
    (root / "ingest" / "domain_b" / "source_a").mkdir(parents=True)
    (root / "transform").mkdir(parents=True)

    return root


def test_ls_finds_jobs(root_dir):
    runner = CliRunner()
    result = runner.invoke(cli, ["ls", str(root_dir)])
    assert result.exit_code == 0
    assert "domain_a" in result.output


def test_ls_empty_directory(tmp_path: Path):
    root = tmp_path / "my_project"
    (root / "ingest").mkdir(parents=True)
    runner = CliRunner()

    result = runner.invoke(cli, ["ls", str(root)])

    assert result.exit_code == 0
    assert "No jobs found" in result.output
