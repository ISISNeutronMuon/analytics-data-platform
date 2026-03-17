"""Tests for elt_common.manifest"""

import textwrap
from pathlib import Path

import pytest

from elt_common.manifest import JobManifest, TableConfig, load_manifest, discover_jobs


class TestLoadManifest:
    def test_loads_valid_manifest(self, tmp_path: Path):
        toml = textwrap.dedent("""\
            [job]
            name = "my_job"
            domain = "testing"
            warehouse = "landing"

            [extract]
            entrypoint = "my_job:extract"

            [[tables]]
            name = "measurements"
            write_mode = "merge"
            merge_on = ["id", "ts"]

            [tables.partition]
            ts = "month"

            [[tables]]
            name = "events"
            write_mode = "append"
        """)
        (tmp_path / "elt.toml").write_text(toml)

        manifest = load_manifest(tmp_path)

        assert manifest.name == "my_job"
        assert manifest.domain == "testing"
        assert manifest.warehouse == "landing"
        assert manifest.namespace == "testing_my_job"
        assert manifest.extract_entrypoint == "my_job:extract"
        assert len(manifest.tables) == 2

        t0 = manifest.tables[0]
        assert t0.name == "measurements"
        assert t0.write_mode == "merge"
        assert tuple(t0.merge_on) == ("id", "ts")
        assert t0.partition == {"ts": "month"}

        t1 = manifest.tables[1]
        assert t1.name == "events"
        assert t1.write_mode == "append"

    def test_missing_job_name_raises(self, tmp_path: Path):
        (tmp_path / "elt.toml").write_text(
            textwrap.dedent("""\
            [job]
            domain = "testing"
            warehouse = "landing"

            [extract]
            entrypoint = "x:extract"
        """)
        )
        with pytest.raises(ValueError, match="job.name"):
            load_manifest(tmp_path)

    def test_missing_extract_entrypoint_raises(self, tmp_path: Path):
        (tmp_path / "elt.toml").write_text(
            textwrap.dedent("""\
            [job]
            name = "x"
            domain = "testing"
            warehouse = "landing"

            [extract]
        """)
        )
        with pytest.raises(ValueError, match="extract.entrypoint"):
            load_manifest(tmp_path)

    def test_missing_elt_toml_raises(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            load_manifest(tmp_path)

    def test_transform_section_parsed(self, tmp_path: Path):
        toml = textwrap.dedent("""\
            [job]
            name = "x"
            domain = "d"
            warehouse = "w"

            [extract]
            entrypoint = "x:extract"

            [[tables]]
            name = "t"

            [transform]
            dbt_dir = "path/to/transform"
            dbt_select = "models/staging"
        """)
        (tmp_path / "elt.toml").write_text(toml)
        manifest = load_manifest(tmp_path)

        assert manifest.transform is not None
        assert manifest.transform.dbt_dir == "path/to/transform"
        assert manifest.transform.dbt_select == "models/staging"

    def test_source_config_accessible(self, tmp_path: Path):
        toml = textwrap.dedent("""\
            [job]
            name = "x"
            domain = "d"
            warehouse = "w"

            [extract]
            entrypoint = "x:extract"

            [[tables]]
            name = "t"

            [source.sharepoint]
            site_url = "https://example.com"
        """)
        (tmp_path / "elt.toml").write_text(toml)
        manifest = load_manifest(tmp_path)

        assert manifest.source_config["sharepoint"]["site_url"] == "https://example.com"


class TestDiscoverJobs:
    def test_discovers_nested_jobs(self, tmp_path: Path):
        for name in ("job_a", "job_b"):
            job_dir = tmp_path / "ingest" / name
            job_dir.mkdir(parents=True)
            (job_dir / "elt.toml").write_text(
                textwrap.dedent(f"""\
                [job]
                name = "{name}"
                domain = "test"
                warehouse = "w"

                [extract]
                entrypoint = "{name}:extract"

                [[tables]]
                name = "t"
            """)
            )

        manifests = discover_jobs(tmp_path)
        assert len(manifests) == 2
        names = [m.name for m in manifests]
        assert "job_a" in names
        assert "job_b" in names

    def test_returns_empty_when_no_jobs(self, tmp_path: Path):
        assert discover_jobs(tmp_path) == []
