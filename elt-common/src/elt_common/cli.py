"""``elt`` CLI — the main entry point for running ingest jobs.

Commands::

    elt run <job_dir>        Run an ingest job
    elt ls [--warehouse W]   List discovered jobs
    elt validate [job_dir]   Validate elt.toml files
    elt test <job_dir>       Run tests for a single job
    elt test-all <root>      Run tests for all jobs under a root directory
"""

import logging
import subprocess
import sys
from pathlib import Path

import click

from elt_common.manifest import discover_jobs, load_manifest
from elt_common.runner import run_job


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    default="INFO",
    help="Set the logging level.",
)
def cli(log_level: str) -> None:
    """ELT pipeline runner for Iceberg data warehouses."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@cli.command()
@click.argument("job_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--step",
    type=click.Choice(["all", "ingest", "transform"], case_sensitive=False),
    default="all",
    help="Which step(s) to run.",
)
def run(job_dir: Path, step: str) -> None:
    """Run an ELT job from the given directory."""
    run_job(job_dir, steps=step)


@cli.command()
@click.argument("root", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--warehouse", default=None, help="Filter by warehouse name.")
def ls(root: Path, warehouse: str | None) -> None:
    """List all discovered elt jobs under ROOT."""
    manifests = discover_jobs(root)
    if warehouse:
        manifests = [m for m in manifests if m.warehouse == warehouse]

    if not manifests:
        click.echo("No jobs found.")
        return

    # Header
    click.echo(f"{'Name':<30} {'Domain':<20} {'Warehouse':<20} {'Tables'}")
    click.echo("-" * 80)
    for m in manifests:
        table_names = ", ".join(t.name for t in m.tables)
        click.echo(f"{m.name:<30} {m.domain:<20} {m.warehouse:<20} {table_names}")


@cli.command()
@click.argument(
    "job_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=False,
    default=None,
)
def validate(job_dir: Path | None) -> None:
    """Validate elt.toml file(s).

    If JOB_DIR is given, validate just that directory.
    Otherwise validate all elt.toml files found under the current directory.
    """
    if job_dir:
        dirs = [job_dir]
    else:
        dirs = [p.parent for p in Path(".").rglob("elt.toml")]

    errors = 0
    for d in dirs:
        try:
            manifest = load_manifest(d)
            click.echo(f"  OK  {d} ({manifest.name})")
        except Exception as exc:
            click.echo(f"  FAIL  {d}: {exc}", err=True)
            errors += 1

    if errors:
        raise SystemExit(1)


@cli.command()
@click.argument("job_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--catalog-type", default="rest", help="Catalog type for e2e tests (sql or rest).")
@click.argument("pytest_args", nargs=-1, type=click.UNPROCESSED)
def test(job_dir: Path, catalog_type: str, pytest_args: tuple[str, ...]) -> None:
    """Run tests for an ELT job.

    Discovers and runs pytest in <job_dir>/tests/.
    Extra arguments after -- are forwarded to pytest.
    """
    test_dir = job_dir / "tests"
    if not test_dir.is_dir():
        raise click.ClickException(f"No tests/ directory found in {job_dir}")

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        str(test_dir),
        f"--catalog-type={catalog_type}",
        "-v",
        *pytest_args,
    ]
    result = subprocess.run(cmd, cwd=str(job_dir))
    raise SystemExit(result.returncode)


@cli.command("test-all")
@click.argument("root", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--catalog-type", default="rest", help="Catalog type for e2e tests (sql or rest).")
def test_all(root: Path, catalog_type: str) -> None:
    """Run tests for all jobs that have a tests/ directory under ROOT."""
    test_dirs = sorted(
        p.parent for p in root.rglob("tests/conftest.py") if (p.parent.parent / "elt.toml").exists()
    )
    if not test_dirs:
        click.echo("No job test directories found.")
        return

    failures: list[Path] = []
    for td in test_dirs:
        job_dir = td.parent
        click.echo(f"\n{'=' * 60}\nTesting: {job_dir}\n{'=' * 60}")
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                str(td),
                f"--catalog-type={catalog_type}",
                "-v",
            ],
            cwd=str(job_dir),
        )
        if result.returncode != 0:
            failures.append(job_dir)

    if failures:
        click.echo(f"\nFailed: {', '.join(str(f) for f in failures)}")
        raise SystemExit(1)
    click.echo("\nAll job tests passed.")
