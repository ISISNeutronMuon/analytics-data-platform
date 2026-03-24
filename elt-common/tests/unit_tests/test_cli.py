"""Tests for the elt CLI"""

import textwrap
from pathlib import Path

from click.testing import CliRunner
from elt_common.cli import cli
import pytest


@pytest.fixture
def job_dir(tmp_path: Path) -> Path:
    """Create a minimal valid job directory."""
    toml = textwrap.dedent("""\
        [job]
        name = "test_job"
        domain = "testing"
        warehouse = "test_wh"

        [extract]
        entrypoint = "test_extract:extract"

        [[tables]]
        name = "data"
        write_mode = "append"
    """)
    (tmp_path / "elt.toml").write_text(toml)
    return tmp_path


class TestValidateCommand:
    def test_validate_valid_manifest(self, job_dir):
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", str(job_dir)])
        assert result.exit_code == 0
        assert "OK" in result.output

    def test_validate_invalid_manifest(self, tmp_path):
        (tmp_path / "elt.toml").write_text("[job]\n")
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", str(tmp_path)])
        assert result.exit_code == 1
        assert "FAIL" in result.output


class TestLsCommand:
    def test_ls_finds_jobs(self, job_dir):
        runner = CliRunner()
        result = runner.invoke(cli, ["ls", str(job_dir.parent)])
        assert result.exit_code == 0
        assert "test_job" in result.output

    def test_ls_filters_by_warehouse(self, job_dir):
        runner = CliRunner()
        result = runner.invoke(cli, ["ls", str(job_dir.parent), "--warehouse", "nonexistent"])
        assert result.exit_code == 0
        assert "No jobs found" in result.output

    def test_ls_empty_directory(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(cli, ["ls", str(tmp_path)])
        assert result.exit_code == 0
        assert "No jobs found" in result.output
