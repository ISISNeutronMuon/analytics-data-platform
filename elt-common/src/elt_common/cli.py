"""``elt`` CLI — the main entry point for running elt jobs."""

import logging
from pathlib import Path

import click

from elt_common.pipeline import PipelinesProject


@click.group()
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    default="INFO",
    help="Set the logging level.",
)
def cli(log_level: str) -> None:
    """ELT pipeline runner for Iceberg-based warehouses."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@cli.command()
@click.argument("root", type=click.Path(exists=True, file_okay=False, path_type=Path))
def ls(root: Path) -> None:
    """List all discovered elt jobs under ROOT."""
    pipeline = PipelinesProject(root)

    if not pipeline.ingest_jobs:
        click.echo("No jobs found.")
        return

    # Header
    click.echo(f"Pipeline: {pipeline.name}")
    click.echo()
    click.echo(f"{'Name':<30} {'Domain':<20}")
    click.echo("-" * 50)
    for p in pipeline.ingest_jobs:
        click.echo(f"{p.name:<30} {p.domain:<20}")


@cli.command()
@click.argument("root", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument("job_name", type=str)
@click.option(
    "--step",
    type=click.Choice(["all", "ingest", "transform"], case_sensitive=False),
    default="all",
    help="Which step to run.",
)
def run(root: Path, job_name: str, step: str) -> None:
    """Run a named ELT job given the root of the ELT project and the name of the job.

    job_name must be fully qualified with the domain name from the ingest directory,
    e.g. domain.job.
    """
    project = PipelinesProject(root)
    project.run_job(job_name)
