"""Pull raw data from defined beamlines and compute monitor peak positions

It currently requires the ISIS archive to be mounted locally.
"""

import logging
from collections import namedtuple
import functools
from pathlib import Path
from typing import Any, Dict, Literal, Sequence, Iterator

from pydantic_settings import BaseSettings

from elt_common.extract import (
    BaseExtract,
    ResourceProperties,
    ResourceWriteProperties,
)
from fit_monitor import (
    MonitorFitConfig,
    MonitorPeak,
    fit_monitor_peak,
    gaussian_plus_flat,
)
import numpy as np
import pyarrow as pa

LOGGER = logging.getLogger(__name__)

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
    )
}

RUNS_CONFIG: Dict[str, Any] = {"pearl": {"cycle_start": "15_2", "skip": [95382]}}


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
    LOGGER.debug(
        f"Finding available runs (mode={run_mode}) for {beamline} starting at cycle {cycle_start}"
    )

    data_dir = archive_mount / f"NDX{beamline}" / "Instrument" / "data"
    if not data_dir.exists():
        raise ValueError(f"Data directory does not exist: {data_dir}")

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
    if not cycle_years:
        LOGGER.warning("No cycles directory found.")
        return {}

    if run_mode == "incremental":
        cycle_years = [cycle_years[0]]

    available_runs = {}
    for cycle_year in cycle_years:
        cycle_dir = f"{CYCLE_DIR_PREFIX}{cycle_year[2:]}"
        LOGGER.debug(f"Checking cycle {cycle_dir}")
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
                LOGGER.warning(f"Could not parse run number from {file.name}")
                continue

        if cycle_runs:
            available_runs[cycle_dir] = sorted(cycle_runs)
            LOGGER.debug(f"Found {len(cycle_runs)} runs in {cycle_dir}")

        # Stop if we've reached the cycle_start
        if cycle_start in cycle_dir:
            break

    LOGGER.debug(f"Found {len(available_runs)} cycles.")
    return available_runs


def monitor_peaks(archive_mount: str, run_mode: RunMode = "incremental"):
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

    for beamline, fit_config in FIT_CONFIGS.items():
        LOGGER.info(f"Fitting monitor peaks for '{beamline}'")
        beamline_runs = RUNS_CONFIG[beamline.lower()]
        archive = Path(archive_mount)
        available_runs = find_available_runs_from_archive(
            run_mode,
            archive,
            beamline,
            beamline_runs["cycle_start"],
            beamline_runs["skip"],
        )
        for cycle, runs in available_runs.items():
            if not runs:
                continue
            LOGGER.debug(f"Fitting runs {runs[0].run_number} -> {runs[-1].run_number}")
            peaks = [fit_monitor_peak(run.path, fit_config) for run in runs]
            yield pa.Table.from_pylist([as_dict(cycle, peak) for peak in peaks if peak])


class Configuration(BaseSettings):
    archive_mount: str
    run_mode: RunMode = "incremental"


class Extract(BaseExtract):
    config_cls = Configuration

    def extract_resource_properties(self) -> Iterator[tuple[str, ResourceProperties]]:
        yield (
            "monitor_peaks",
            ResourceProperties(
                extractor=lambda _: monitor_peaks(
                    self.config.archive_mount, self.config.run_mode
                ),
                write_properties=ResourceWriteProperties(
                    write_mode="merge",
                    merge_on=["beamline", "run_number"],
                    partition={"beamline": "identity", "run_start": "month"},
                ),
            ),
        )
