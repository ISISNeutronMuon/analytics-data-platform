import contextlib
import logging

from dbt.exceptions import UninstalledPackagesFoundError

from elt_common.pipeline import PipelinesProject
from elt_common.pipeline_types import ELTIngestManifest

LOGGER = logging.getLogger(__name__)
_common_excludes = ["--exclude", "resource_type:unit_test", "--exclude", "resource_type:test"]


def _make_dbt_args(ingest: ELTIngestManifest, remote: bool):
    args = ["run", "--select", f"source:{ingest.destination_namespace}+", *_common_excludes]
    if remote:
        args.extend(["--profile", "remote"])
    return args


def run_transform(project: PipelinesProject, ingest: ELTIngestManifest, remote: bool = False):
    """Transform the data ingested from the specified source"""
    args = _make_dbt_args(ingest, remote)
    LOGGER.debug(f"Invoking 'dbt {' '.join(args)}'")

    from dbt.cli.main import dbtRunner

    with contextlib.chdir(project.transform_dir):
        runner = dbtRunner()
        result = runner.invoke(args)
        if result.success or not isinstance(result.exception, UninstalledPackagesFoundError):
            return result

        # dbt dependencies weren't installed. Try installing them and rerunning
        LOGGER.info("Installing dbt dependencies")
        runner.invoke(["deps"])

        LOGGER.info(f"Retrying 'dbt {' '.join(args)}'")
        return runner.invoke(args)
