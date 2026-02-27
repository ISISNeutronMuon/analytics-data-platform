"""Pull raw data from defined beamlines and compute monitor peak positions

It currently requires the ISIS archive to be mounted locally.
"""

from collections import namedtuple
import functools
from pathlib import Path
from typing import Dict, Literal, Sequence

import dlt
import dlt.common.logger as logger
import elt_common.cli as cli_utils
from elt_common.dlt_destinations.pyiceberg.helpers import load_iceberg_table
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    PartitionTrBuilder,
)
from fit_monitor import (
    MonitorFitConfig,
    MonitorPeak,
    fit_monitor_peak,
    gaussian_plus_flat,
)
import numpy as np
from pyiceberg.expressions import EqualTo
import requests
import xml.etree.ElementTree as ET

RunFile = namedtuple("RunFile", ("run_number", "path"))
RunMode = Literal["backfill", "incremental"]

CYCLE_DIR_PREFIX = "cycle_"
FIT_CONFIGS = {
    "PEARL": MonitorFitConfig(
        beamline="PEARL",
        curve_fit_args={
            "x_range": (3800, 6850),
            "function": functools.partial(gaussian_plus_flat, constant=16.6099),
            "p0": [
                19.2327,  # amplitude
                4843.8,  # mu (peak centre)
                1532.64,  # sigma
            ],
            "bounds": (
                (-np.inf, 4600, 1100),
                (np.inf, 5200, 1900),
            ),
        },
        cycle_start="25_4",
        skip_runs=(95382,),
    )
}


def get_fitted_runs(beamline: str, pipeline: dlt.Pipeline) -> Dict[str, Sequence[int]]:
    """Retrieve the list of run numbers that have already been loaded to the warehouse"""
    existing_peaks_table = load_iceberg_table(pipeline, "monitor_peaks")
    if existing_peaks_table is None:
        logger.debug("Peaks table does not exist. No runs have been loaded.")
        return {}

    logger.debug("Peaks table exists, finding loaded runs.")
    fitted_runs = {}
    c_beamline, c_cycle, c_run_number = "beamline", "cycle_name", "run_number"
    beamline_peaks = existing_peaks_table.scan(
        row_filter=EqualTo(c_beamline, beamline),
        selected_fields=(c_run_number, c_cycle),
    ).to_arrow()
    if beamline_peaks.num_rows > 0:
        cycle_groups = beamline_peaks.group_by(c_cycle).aggregate(
            [(c_run_number, "list")]
        )
        fitted_runs = dict(
            zip(
                cycle_groups[c_cycle].to_pylist(),
                sorted(cycle_groups[c_run_number + "_list"].to_pylist()),
            )
        )
    else:
        logger.debug(f"No runs loaded for '{beamline}'.")

    return fitted_runs


def find_available_runs_from_archive(
    run_mode: RunMode,
    archive_mount: Path,
    beamline: str,
    cycle_start: str,
    skip: Sequence[int],
) -> Dict[str, Sequence[RunFile]]:
    """Look over the archive for the beamline and find the available runs

    If the mode=incremental only look at the most recent cycle.
    """
    logger.debug(
        f"Finding available runs (mode={run_mode}) for {beamline} starting at cycle {cycle_start}"
    )

    data_dir = archive_mount / f"NDX{beamline}" / "Instrument" / "data"
    if not data_dir.exists():
        logger.warning(f"Data directory does not exist: {data_dir}")
        return {}

    # Get all cycle directories
    # To sort by correctly we need to pad the year to full YYYY
    cycle_dirs = [
        d.name[len(CYCLE_DIR_PREFIX) :]
        for d in data_dir.iterdir()
        if d.is_dir() and d.name.startswith(CYCLE_DIR_PREFIX)
    ]
    cycle_years = sorted(
        map(lambda x: f"{19}{x}" if x.startswith("9") else f"{20}{x}", cycle_dirs),
        reverse=True,
    )

    if run_mode == "incremental":
        cycle_years = [cycle_years[0]]

    available_runs = {}
    for cycle_year in cycle_years:
        cycle_dir = f"{CYCLE_DIR_PREFIX}{cycle_year[2:]}"
        logger.debug(f"Checking cycle {cycle_dir}")
        cycle_path = data_dir / cycle_dir

        # Find all .nxs files and extract run numbers
        cycle_runs = []
        for file in cycle_path.glob(f"{beamline}*.nxs"):
            try:
                run_str = file.stem[len(beamline) :]
                run_number = int(run_str)
                if run_number not in skip:
                    cycle_runs.append(RunFile(run_number, file))
            except (ValueError, IndexError):
                logger.warning(f"Could not parse run number from {file.name}")
                continue

        if cycle_runs:
            available_runs[cycle_dir] = sorted(cycle_runs)
            logger.debug(f"Found {len(cycle_runs)} runs in {cycle_dir}")

        # Stop if we've reached the cycle_start
        if cycle_start in cycle_dir:
            break

    logger.debug(f"Found {len(available_runs)} cycles.")
    return available_runs


@dlt.resource(
    merge_key=["beamline", "run_number"],
    write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def monitor_peaks(
    archive_mount: str = dlt.config.value,
    run_mode: RunMode = "incremental",
):
    # This defines the column order
    def as_dict(cycle_name: str, peak: MonitorPeak):
        return {
            "beamline": peak.run.beamline,
            "run_number": peak.run.run_number,
            "cycle_name": cycle_name,
            "run_start": peak.run.start_time,
            "proton_charge": peak.run.proton_charge_uamps,
            "peak_centre": peak.centre,
            "peak_centre_error": peak.centre_error,
            "peak_amplitude": peak.amplitude,
            "peak_amplitude_error": peak.amplitude_error,
            "peak_sigma": peak.sigma,
            "peak_sigma_error": peak.sigma_error,
        }

    pipeline = dlt.current.pipeline()
    for beamline, fit_config in FIT_CONFIGS.items():
        logger.info(f"Fitting monitor peaks for '{beamline}'")
        archive = Path(archive_mount)
        available_runs = find_available_runs_from_archive(
            run_mode,
            archive,
            beamline,
            fit_config.cycle_start,
            fit_config.skip_runs,
        )
        runs_already_fitted = get_fitted_runs(beamline, pipeline)
        runs_to_fit = {
            key: [
                x
                for x in available_runs[key]
                if x.run_number not in set(runs_already_fitted.get(key, []))
            ]
            for key in available_runs
        }
        for cycle, runs in runs_to_fit.items():
            logger.debug(f"Fitting runs {runs[0].run_number} -> {runs[-1].run_number}")
            peaks = [fit_monitor_peak(run.path, fit_config) for run in runs]
            yield [as_dict(cycle, peak) for peak in peaks if peak]


if __name__ == "__main__":
    cli_utils.cli_main_v2(
        pipeline_name="moderator_performance",
        source_domain="beamlines",
        data_generator=pyiceberg_adapter(
            monitor_peaks,
            partition=[
                PartitionTrBuilder.identity("beamline"),
                PartitionTrBuilder.month("run_start"),
            ],
        ),
    )
