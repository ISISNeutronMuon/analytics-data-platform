"""``elt`` CLI — the main entry point for running elt jobs."""

import logging
import sys
import time
from pathlib import Path
from typing import Literal, Optional, cast

import click

from elt_common.pipeline import PipelinesProject
from elt_common.pipeline_types import ELTIngestManifest
from elt_common.ingest import run_ingest
from elt_common.transform import run_transform

LOGGER = logging.getLogger(__name__)


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


StepOption = Literal["all", "ingest", "transform"]
Step = Literal["ingest", "transform"]


@cli.command()
@click.argument("root", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument("job_name", type=str)
@click.option(
    "--step",
    type=click.Choice(["all", "ingest", "transform"], case_sensitive=False),
    default="all",
    help="Whether to run the ingest step, the transform step, or both.",
)
def run(root: Path, job_name: str, step: StepOption = "all") -> None:
    """Run a named ELT job given the root of the ELT project and the name of the job.

    If JOB_NAME is unique it can be specified unqualified, otherwise it must be
    qualified with the domain name from the ingest directory, e.g. domain.job.

    Runs both the 'ingest' (extract and load) and 'transform' steps unless
    restricted by the 'step' option.
    """

    project = PipelinesProject(root)

    job = _find_matching_ingest_job(project, job_name)
    if not job:
        sys.exit(1)

    # cast because some type checkers can't figure this out, unfortunately
    steps = cast(list[Step], ["ingest", "transform"] if step == "all" else [step])
    for s in steps:
        _run_step(s, job, project)


def _find_matching_ingest_job(
    project: PipelinesProject, job_name: str
) -> Optional[ELTIngestManifest]:
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


def _run_step(
    step: Literal["ingest", "transform"],
    manifest: ELTIngestManifest,
    project: PipelinesProject,
):
    LOGGER.info(f"Starting {step} job: {manifest.full_name}")

    t0 = time.monotonic()

    if step == "ingest":
        run_ingest(manifest)

    elif step == "transform":
        result = run_transform(project, manifest)
        if not result.success:
            if result.exception:
                click.echo("failed with dbt exception:", err=True)
                click.echo(str(result.exception), err=True)
            sys.exit(1)

    elapsed = time.monotonic() - t0
    LOGGER.info(f"{manifest.full_name} {step} completed in {elapsed:.1f}s")


if __name__ == "__main__":
    cli()
