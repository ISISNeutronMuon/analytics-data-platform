"""``elt`` CLI — the main entry point for running ingest jobs."""

import logging
from pathlib import Path

import click

from elt_common.manifest import discover_jobs
from elt_common.runner import run_job
from elt_common.tester import unit_test_job, e2e_test_job


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
    click.echo(f"{'Name':<30} {'Domain':<20} {'Warehouse':<20}")
    click.echo("-" * 70)
    for m in manifests:
        click.echo(f"{m.name:<30} {m.domain:<20} {m.warehouse:<20}")


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
@click.argument("job_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--type", type=click.Choice(["unit", "e2e"], case_sensitive=False), default="unit")
@click.argument("extra_args", nargs=-1, type=click.UNPROCESSED)
def test(job_dir: Path, type: str, extra_args: tuple[str, ...]) -> None:
    """Run tests for an ELT job.

    There are two types of test:
      - unit_tests: Unit tests for just the extract functionality in <job_dir>/unit_tests/. Extra arguments after '--' are forwarded to pytest.
      - e2e_tests: End-to-end tests, standing up a test catalog and running ingest & transforms steps.
    """
    if type == "unit":
        test_dir = job_dir / "unit_tests"
        if not test_dir.is_dir():
            raise click.ClickException(f"No tests/ directory found in {job_dir}")

        result = unit_test_job(test_dir, extra_args)
    else:
        result = e2e_test_job(job_dir)

    raise SystemExit(result)
