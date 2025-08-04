from collections.abc import Generator
import itertools
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple
import subprocess as subp
import sys
from zoneinfo import ZoneInfo

import dlt
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.extract import DltResource
from dlt.pipeline.exceptions import PipelineStepFailed

import humanize
import pandas as pd
import pendulum
import requests

import pipelines_common.cli as cli_utils
import pipelines_common.logging as logging_utils
import pipelines_common.pipeline as pipeline_utils


from pipelines_common.constants import (
    MICROSECONDS_STR,
    MICROSECONDS_STR_INFLUX,
    MICROSECONDS_PER_SEC,
)

# Runtime
LOGGER = logging.getLogger(__name__)
PIPELINE_BASENAME = "influxdb"

# Source
DATASET_NAME = "machinestate"
SCHEMA_NAME_DELIMITER = "::"
INFLUXDB_MACHINESTATE_BACKFILL_START = pendulum.DateTime(
    2018, 1, 1, tzinfo=pendulum.UTC
)

# Staging destination
COLUMN_CHANNEL_NAME = "channel"
LOADER_FILE_FORMAT = "parquet"


def _to_utc_str(timestamp: pendulum.DateTime) -> str:
    return timestamp.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class InfluxQuery:
    """Perform an influx query on a named bucket at a url and with the given auth token"""

    def __init__(self, bucket_name: str, base_url: str, auth_token: str):
        self._bucket_name = bucket_name
        self._base_url = base_url
        self._auth_token = auth_token

    def channel_names(self) -> List[str]:
        def flatten(nested):
            return itertools.chain.from_iterable(nested)

        return list(flatten(self._query("SHOW MEASUREMENTS")["values"]))

    def last_time(
        self,
        channel_name: str,
    ) -> pendulum.DateTime:
        query = f'SELECT LAST("value") FROM "{channel_name}"'
        timestamp_us = self._query(query)["values"][0][0]
        return pendulum.from_timestamp(
            timestamp_us / MICROSECONDS_PER_SEC,
            tz="UTC",
        )

    def select_channel(
        self,
        channel_name: str,
        time_start: pendulum.DateTime,
        time_end: pendulum.DateTime,
    ) -> Optional[pd.DataFrame]:
        query_t0, query_t1 = _to_utc_str(time_start), _to_utc_str(time_end)
        query = f"SELECT \"value\" FROM \"{channel_name}\" WHERE time >= '{query_t0}' AND time < '{query_t1}'"
        try:
            series = self._query(query)
            df = pd.DataFrame.from_records(series["values"], columns=series["columns"])
        except ValueError:
            return None

        df["time"] = pd.to_datetime(
            df["time"],
            origin="unix",
            unit=MICROSECONDS_STR,
            utc=True,
        )
        df["value"] = df["value"].astype("float64")
        df[COLUMN_CHANNEL_NAME] = channel_name
        # Sort by time so the incremental loading mechanism gets the correct "last value"
        df.sort_values(by=["time"], inplace=True)
        return df

    # Private
    def _query(self, query: str) -> Any:
        """Perform a query against the REST api of the influx database. Returns the series field if present"""
        LOGGER.debug(query)
        resp = requests.get(
            f"{self._base_url}/query",
            headers={"Authorization": f"Token {self._auth_token}"},
            params={
                "db": self._bucket_name,
                "epoch": MICROSECONDS_STR_INFLUX,
                "q": query,
            },
        )
        resp_json = resp.json()
        try:
            return resp_json["results"][0]["series"][0]
        except (IndexError, KeyError):
            raise ValueError(
                f"Error querying Influx. Unexpected result structure:\n '{resp_json}'"
            )


@dlt.resource()
def influxdb_get_measurement(
    influx: InfluxQuery,
    channel_name: str,
    time: dlt.sources.incremental = dlt.sources.incremental("time"),
):
    """Load data for a given channel."""

    def next_chunk_start(ts: pendulum.DateTime) -> pendulum.DateTime:
        # A bug in Pendulum drops the timezone on addition with days:
        # https://github.com/python-pendulum/pendulum/issues/669
        return (ts.add(weeks=52)).astimezone(pendulum.UTC)

    def next_microsecond(ts: pendulum.DateTime) -> pendulum.DateTime:
        return (ts.add(microseconds=1)).astimezone(pendulum.UTC)

    # The start time of the load is determined through the dlt.incremental mechanism tracking the
    # value of the last load. If no end time is specified the last time within Influx
    # is queried and a microsecond is added to ensure we capture all values

    time_start = time.start_value
    time_end = (
        time.end_value
        if time.end_value is not None
        else next_microsecond(influx.last_time(channel_name))
    )
    LOGGER.debug(
        f"Pulling measurement '{channel_name}' for time range [{time_start}, {time_end})"
    )

    chunk_start, chunk_end = time_start, min(next_chunk_start(time_start), time_end)
    while chunk_start < chunk_end:
        df = influx.select_channel(channel_name, chunk_start, chunk_end)
        yield df
        # next chunk
        chunk_start = chunk_end
        chunk_end = min(next_chunk_start(chunk_end), time_end)


def machinestate_source_factory(
    schema_name: str,
    channels: Sequence[str],
    influx: InfluxQuery,
    backfill_range: Tuple[pendulum.DateTime, Optional[pendulum.DateTime]],
):

    @dlt.source(name=schema_name, parallelized=True, max_table_nesting=0)
    def machinestate() -> Generator[DltResource]:

        time_end_value = backfill_range[1] if backfill_range[1] is not None else None
        for channel_name in channels:
            initial_time_value = backfill_range[0]
            time_args = {
                "time": dlt.sources.incremental(
                    initial_value=initial_time_value, end_value=time_end_value
                )
            }
            table_name = channel_name
            yield influxdb_get_measurement(influx, channel_name, **time_args).with_name(
                table_name
            ).apply_hints(table_name=table_name)

    return machinestate()


def extract_and_load_machinestate(
    pipeline: dlt.Pipeline,
    influx: InfluxQuery,
    schema_name: str,
    channels: Sequence[str],
    on_pipeline_step_failed: str,
):
    """Extract and load the Influxdb machinestate channels to the destination

    :param pipeline: A dlt.pipeline object configured with the appropriate destination
    :param influx: An InfluxQuery instance
    :param schema_name: The name of the source schema
    :param channels: A list of channels to load.
    :param on_pipeline_step_failed: What action to take on a pipeline step failure: options=(raise, log_and_continue)
    """
    load_info = None
    try:
        load_info = pipeline.run(
            machinestate_source_factory(
                schema_name,
                channels,
                influx,
                backfill_range=(INFLUXDB_MACHINESTATE_BACKFILL_START, None),
            ),
            loader_file_format=LOADER_FILE_FORMAT,
            write_disposition="append",
        )
        LOGGER.debug(load_info)
    except PipelineStepFailed as step_exc:
        if on_pipeline_step_failed == "raise":
            raise
        else:
            LOGGER.info(f"Pipeline step failed with error: {str(step_exc)}")
            LOGGER.debug(f"  {str(step_exc.exception)}")

            # If any packages failed to load we don't want to load them again.
            pipeline.drop_pending_packages(with_partial_loads=True)

    return load_info


def create_pipeline(pipeline_name_fq: str, dataset_name: str):
    return dlt.pipeline(
        destination="filesystem",
        pipeline_name=pipeline_name_fq,
        dataset_name=dataset_name,
        progress="log",
    )


def run_pipeline(
    pipeline: dlt.Pipeline,
    influx: InfluxQuery,
    schema_name: str,
    channels_to_load: Sequence[str],
    on_pipeline_step_failure: str,
):
    elt_started_at = pendulum.now()

    for channels_batch in itertools.batched(
        channels_to_load, dlt.config["sources.channel_batch_size"]
    ):
        load_info = extract_and_load_machinestate(
            pipeline, influx, schema_name, channels_batch, on_pipeline_step_failure
        )
        if load_info is not None:
            if LOGGER.level == logging.DEBUG:
                for load_id in load_info.loads_ids:
                    LOGGER.debug(pipeline.get_load_package_info(load_id))

    elt_ended_at = pendulum.now()
    LOGGER.info(
        f"ELT for schema {schema_name} completed in {
        humanize.precisedelta(
            elt_ended_at - elt_started_at
        )}"
    )

    return pipeline


# ------------------------------------------------------------------------------
def get_channels_to_load(
    cli_args: List[str],
    influx: InfluxQuery,
    pipeline: dlt.Pipeline,
    skip_existing: bool,
) -> Dict[str, List[str]]:
    """Determine which channels should be loaded, given the arguments from the cli.

    The return value is a list of dictionaries where each item in the list contains
    roughly the same number of channels. This aids batching up the loading process.

    :param cli_args: Arguments from the command line. These take precendence
    :param influx: InfluxQuery object
    :param pipeline: A dlt.Pipeline object that can query the destination
    :param skip_existing: If true, filter out channels that already exist in the destination
    :return: A map of {schema_name: [channels]}. Schema name is defined as the string before the ::
    """
    if cli_args:
        channels_to_load = cli_args
    else:
        channels_to_load = influx.channel_names()

    if skip_existing:
        with inject_section(ConfigSectionContext(pipeline.pipeline_name)):
            existing_tables = pipeline.dataset().schema.data_table_names(
                seen_data_only=True
            )
            existing_channels = set(
                map(
                    lambda x: x.replace("_", "::", 1).replace("_", ":"), existing_tables
                )
            )
            channels_to_load = list(
                set(map(lambda x: x.lower(), channels_to_load)) - set(existing_channels)
            )

    schema_to_channels: Dict[str, List[str]] = {}
    for channel in channels_to_load:
        if SCHEMA_NAME_DELIMITER not in channel:
            LOGGER.warning(
                f"Channel '{channel}' does not contain expected '{SCHEMA_NAME_DELIMITER}'. Skipping"
            )
            continue
        schema_name = channel.split(SCHEMA_NAME_DELIMITER)[0]
        channels = schema_to_channels.setdefault(schema_name, [])
        channels.append(channel)

    return schema_to_channels


def parse_args() -> cli_utils.argparse.Namespace:
    parser = cli_utils.create_standard_argparser()
    parser.add_argument(
        "--channels",
        nargs="+",
        default=[],
        help="A whitespace-separate list of channel names to load to the warehouse. By default all channels from the source will be loaded.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="If provided then don't query Influx for new data for channels that exist in the destination.",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    logging_utils.configure_logging(args.log_level, keep_records_from=["__main__"])

    pipeline = pipeline_utils.create_pipeline(
        PIPELINE_BASENAME, "filesystem", DATASET_NAME, progress="log"
    )
    LOGGER.info(f"-- Pipeline={pipeline.pipeline_name} --")

    influx = InfluxQuery(
        dlt.config["influxdb.bucket_name"],
        dlt.config["influxdb.base_url"],
        dlt.secrets["influxdb.auth_token"],
    )
    channels_to_load = get_channels_to_load(
        args.channels, influx, pipeline, args.skip_existing
    )
    if not channels_to_load:
        LOGGER.info("No channels have been found to load. Exiting.")
        return

    # If run for a large number of channels then the memory of the main
    # process can creep up. Using subprocesses (but not in parallel)
    # helps keep the memory under control
    schemas_total_count = len(channels_to_load)
    if schemas_total_count == 1:
        schema_name, channels = channels_to_load.popitem()
        LOGGER.info(
            f"Running pipeline for {len(channels)} channel in schema '{schema_name}'"
        )
        run_pipeline(
            pipeline, influx, schema_name, channels, args.on_pipeline_step_failure
        )
    else:
        complete_elt_started_at = pendulum.now()
        for schema_index, (schema_name, channels) in enumerate(
            channels_to_load.items()
        ):
            LOGGER.info(
                f"Starting subprocess for schema {schema_index+1}/{schemas_total_count}"
            )
            cmd = [sys.executable, __file__]
            # Pass all arguments from parent to child with exception of the channels list that we want to be smaller
            cmd.extend(
                filter(
                    lambda x: (x != "--channels" and x not in args.channels),
                    sys.argv[1:],
                )
            )
            cmd.append("--channels")
            cmd.extend(channels)
            LOGGER.debug(f"Executing '{cmd}'")
            subp.run(cmd, check=(args.on_pipeline_step_failure == "raise"))
            LOGGER.info(f"Completed subprocess {schema_index+1}/{schemas_total_count}")

        complete_elt_ended_at = pendulum.now()
        LOGGER.info(
            f"Complete ELT completed in {
            humanize.precisedelta(
                complete_elt_ended_at - complete_elt_started_at
            )}"
        )


if __name__ == "__main__":
    main()
