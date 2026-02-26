"""Utility functions for a cli script"""

import argparse
import logging
import typing
from typing import Any

import dlt
from dlt.common.destination.reference import TDestinationReferenceArg
import dlt.common.logger as logger
from dlt.common.typing import TLoaderFileFormat
from dlt.common.runtime.collector import NULL_COLLECTOR
from dlt.common.schema.typing import TWriteDisposition
from dlt.extract.reference import SourceFactory
from dlt.pipeline.progress import TCollectorArg
import humanize

from .pipeline import dataset_name, dataset_name_v2


def create_standard_argparser(
    default_destination: TDestinationReferenceArg,
    default_loader_file_format: TLoaderFileFormat,
    default_progress: TCollectorArg,
) -> argparse.ArgumentParser:
    """Creates an ArgumentParser with standard options common to most pipelines"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-level",
        choices=logging.getLevelNamesMapping().keys(),
        default=logging.INFO,
    )
    parser.add_argument(
        "--on-pipeline-step-failure",
        type=str,
        default="raise",
        choices=["raise", "log_and_continue"],
        help="What should be done with pipeline step failure exceptions",
    )
    parser.add_argument(
        "--destination",
        default=default_destination,
        help="Destination for the loaded data.",
    )
    parser.add_argument(
        "--write-disposition",
        default=None,
        choices=typing.get_args(TWriteDisposition),
        help="The write disposition used with dlt.",
    )
    parser.add_argument(
        "--loader-file-format",
        default=default_loader_file_format,
        help="The dlt loader file format",
    )
    parser.add_argument(
        "--progress",
        default=default_progress,
        help="The dlt progress option.",
    )

    return parser


def cli_main(
    pipeline_name: str,
    data_generator: Any,
    dataset_name_suffix: str,
    *,
    default_destination: TDestinationReferenceArg = "filesystem",
    default_loader_file_format: TLoaderFileFormat = "parquet",
    default_progress: TCollectorArg = NULL_COLLECTOR,
):
    """Run a standard extract and load pipeline

    :param pipeline_name: Name of dlt pipeline
    :param data_generator: Callable returning a dlt.DltSource or dlt.DltResource
    :param dataset_name_suffix: Suffix part of full dataset name in the destination. The given string is prefixed with
                                a standard string defined in constants.DATASET_NAME_PREFIX_SRCS
    :param default_destination: Default destination, defaults to "filesystem"
    :param default_loader_file_format: Default dlt loader file format, defaults to "parquet"
    :param default_progress: Default progress reporter, defaults to NULL_COLLECTOR
    """
    args = create_standard_argparser(
        default_destination,
        default_loader_file_format,
        default_progress,
    ).parse_args()

    if logger.is_logging():
        logger.LOGGER.setLevel(args.log_level)

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        dataset_name=dataset_name(dataset_name_suffix),
        destination=args.destination,
        progress=args.progress,
    )
    logger.info(f"Starting pipeline: '{pipeline.pipeline_name}'")
    logger.debug("Dropping pending packages to ensure a clean new load")
    pipeline.drop_pending_packages()
    if isinstance(data_generator, SourceFactory):
        data = data_generator()
    else:
        data = data_generator
    pipeline.run(
        data,
        loader_file_format=args.loader_file_format,
        write_disposition=args.write_disposition,
    )
    logger.debug(pipeline.last_trace.last_extract_info)
    logger.debug(f"Extracted row counts: {pipeline.last_trace.last_normalize_info.row_counts}")
    logger.debug(pipeline.last_trace.last_load_info)
    logger.info(
        f"Pipeline {pipeline.pipeline_name} completed in {
            humanize.precisedelta(pipeline.last_trace.finished_at - pipeline.last_trace.started_at)
        }"
    )


def cli_main_v2(
    pipeline_name: str,
    source_domain: str,
    data_generator: Any,
    *,
    default_destination: TDestinationReferenceArg = "elt_common.dlt_destinations.pyiceberg",
    default_loader_file_format: TLoaderFileFormat = "parquet",
    default_progress: TCollectorArg = NULL_COLLECTOR,
):
    """Run a standard extract and load pipeline.

    The full dataset name becomes '${constants.SOURCE_DATASET_NAME_PREFIX}{source_domain}_{pipeline_name}'

    :param pipeline_name: Name of dlt pipeline.
    :param source_domain: Name of domain of the source data
    :param data_generator: Callable returning a dlt.DltSource or dlt.DltResource
    :param default_destination: Default destination, defaults to "filesystem"
    :param default_loader_file_format: Default dlt loader file format, defaults to "parquet"
    :param default_progress: Default progress reporter, defaults to NULL_COLLECTOR
    """
    args = create_standard_argparser(
        default_destination,
        default_loader_file_format,
        default_progress,
    ).parse_args()

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        dataset_name=dataset_name_v2(source_domain, pipeline_name),
        destination=args.destination,
        progress=args.progress,
    )
    if logger.is_logging():
        logger.LOGGER.setLevel(args.log_level)

    logger.info(f"Starting pipeline: '{pipeline.pipeline_name}'")
    logger.debug("Dropping pending packages to ensure a clean new load")
    pipeline.drop_pending_packages()
    if isinstance(data_generator, SourceFactory):
        data = data_generator()
    else:
        data = data_generator
    pipeline.run(
        data,
        loader_file_format=args.loader_file_format,
        write_disposition=args.write_disposition,
    )
    logger.debug(pipeline.last_trace.last_extract_info)
    logger.info(f"Extracted row counts: {pipeline.last_trace.last_normalize_info.row_counts}")
    logger.debug(pipeline.last_trace.last_load_info)
    logger.info(
        f"Pipeline {pipeline.pipeline_name} completed in {
            humanize.precisedelta(pipeline.last_trace.finished_at - pipeline.last_trace.started_at)
        }"
    )
