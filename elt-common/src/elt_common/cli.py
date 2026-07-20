"""``elt`` CLI — the main entry point for running elt jobs."""

import logging
import sys
from pathlib import Path
from typing import Optional

import click

from elt_common.pipeline import PipelinesProject
from elt_common.runner import run_job
from elt_common.pipeline_types import ELTJobManifest


@click.group(context_settings={"show_default": True})
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
    help="Whether to run the ingest step, the transform step, or both. (transform is not currently implemented)",
)
def run(root: Path, job_name: str, step: str = "all") -> None:
    """Run a named ELT job given the root of the ELT project and the name of the job.

    If JOB_NAME is unique it can be specified unqualified, otherwise it must be
    qualified with the domain name from the ingest directory, e.g. domain.job.
    """
    # TODO: transform step as a real option
    steps = ["ingest"] if step == "all" else [step]

    project = PipelinesProject(root)

    if "ingest" in steps:
        job = _find_matching_ingest_job(project, job_name)
        if job:
            run_job(job)
        else:
            sys.exit(1)


def _find_matching_ingest_job(project: PipelinesProject, job_name: str) -> Optional[ELTJobManifest]:
    exact_match = [j for j in project.ingest_jobs if j.full_name == job_name]
    if exact_match:
        return exact_match[0]

    matching_name_jobs = [j for j in project.ingest_jobs if j.name == job_name]
    if not matching_name_jobs:
        click.echo("No job matching that name was found")
    elif len(matching_name_jobs) > 1:
        click.echo(
            f"There are multiple jobs with a matching name: {matching_name_jobs}."
            " Please provide the fully name qualified with the domain."
        )
    else:
        return matching_name_jobs[0]

    return None
