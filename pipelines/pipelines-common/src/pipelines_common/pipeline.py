import os
from typing import cast, Optional

import dlt
from dlt import Pipeline
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.common.libs.pandas import DataFrame
from dlt.pipeline.progress import TCollectorArg, _NULL_COLLECTOR as NULL_COLLECTOR

from pipelines_common.constants import SOURCE_DATASET_NAME_PREFIX

_PIPELINE_ENV_VAR = "PIPELINE_ENV"
_PIPELINE_ENV_NAME_PROD = "production"
_PIPELINE_ENV_NAME_DEV = "develop"


def dataset_name(suffix: str) -> str:
    """Given a suffix return the full dataset name"""
    return f"{SOURCE_DATASET_NAME_PREFIX}{suffix}"


def _pipeline_environment():
    """The configured pipeline name. Defaults to dev"""
    return os.environ.get(_PIPELINE_ENV_VAR, _PIPELINE_ENV_NAME_DEV)


def _is_production() -> bool:
    """Are we running in a production environment based on the
    value of the configured environment variable."""
    return os.environ.get(_PIPELINE_ENV_VAR, "") == _PIPELINE_ENV_NAME_PROD


def _pipeline_prefix() -> str:
    """Return the prefix for the pipeline name in this environment"""
    return "" if _is_production() else f"{_pipeline_environment()}_"


def pipeline_name(basename: str) -> str:
    """Given an unqualified pipeline name,
    optionally prefix it depending on the PIPELINE_ENV environment variable

    :param basename: The unqualified name for the pipeline
    """
    return f"{_pipeline_prefix()}{basename}"


def create_pipeline(
    basename: str,
    destination: str,
    dataset_name: str | None = None,
    progress: TCollectorArg = NULL_COLLECTOR,
) -> Pipeline:
    """Given the base pipeline name return a new Pipeline object using the full qualified name.
    If no dataset name is given the pipeline name is used"""
    pipeline_name_fq = pipeline_name(basename)
    return dlt.pipeline(
        pipeline_name=pipeline_name_fq,
        dataset_name=(dataset_name if dataset_name is not None else basename),
        destination=destination,
        progress=progress,
    )


def find_latest_load_id(pipeline: Pipeline) -> Optional[str]:
    """Check the _dlt_loads table and retrieve the latest load_id"""
    with inject_section(ConfigSectionContext(pipeline_name=pipeline.pipeline_name)):
        ds = pipeline.dataset()
        _dlt_loads_df = cast(DataFrame, ds._dlt_loads.df())
        if len(_dlt_loads_df) > 0:
            return (
                _dlt_loads_df.sort_values("inserted_at", ascending=False).head(1)["load_id"].iat[0]
            )
        else:
            return None
