"""Pull raw data from defined beamlines and compute monitor peak positions

It currently requires the ISIS archive to be mounted locally.
"""

import logging
from collections import namedtuple
import functools
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Literal, Sequence, Iterator, Annotated

from pydantic import Field
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
InstrumentName = Literal["PEARL"]


@dataclass
class InstrumentRunConfig:
    """Optional configuration values that can be specified on a per-instrument basis"""

    cycles: list[Annotated[str, Field(pattern=r"^\d\d_\d$")]] | None = None
    """Exhaustive list of cycles to include"""

    runs: list[int] | None = None
    """Exhaustive list of runs to include"""


InstrumentRunConfigs = dict[InstrumentName, InstrumentRunConfig]

CYCLE_DIR_PREFIX = "cycle_"
FIT_CONFIGS: dict[InstrumentName, MonitorFitConfig] = {
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
    ),
}

FIXED_RUNS_CONFIG: Dict[str, Any] = {"PEARL": {"cycle_start": "15_2", "skip": [95382]}}
"""Values for determining runs to fit which cannot be configured at runtime"""


def find_available_runs_from_archive(
    run_mode: RunMode,
    archive_mount: Path,
    beamline: InstrumentName,
    runs_config: InstrumentRunConfig | None,
) -> Dict[str, Sequence[RunFile]]:
    """Look over the archive for the beamline and find the available runs

    If the mode=incremental only look at the most recent cycle.
    """
    fixed_runs_config = FIXED_RUNS_CONFIG[beamline]
    cycle_start = fixed_runs_config["cycle_start"]
    skip = fixed_runs_config["skip"]

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
    cycles = []
    for cycle_year in cycle_years:
        cycle = cycle_year[2:]
        cycles.append(cycle)
        # Don't use any cycles earlier than cycle_start
        if cycle_start == cycle:
            break

    LOGGER.debug(f"{len(cycles)} total cycle directories")
    if runs_config and runs_config.cycles is not None:
        cycles = [c for c in cycles if c in runs_config.cycles]
        LOGGER.debug(f"{len(cycles)} cycle directories matched instrument config")

    if not cycles:
        LOGGER.warning("No matching cycle directories")
        return {}

    if run_mode == "incremental":
        cycles = [cycles[0]]
        LOGGER.debug(f"Incremental mode, only using most recent cycle {cycles[0]}")

    available_runs = {}
    for cycle in cycles:
        cycle_dir = f"{CYCLE_DIR_PREFIX}{cycle}"
        LOGGER.debug(f"Checking cycle {cycle_dir}")
        cycle_path = data_dir / cycle_dir

        # Find all .nxs files and extract run numbers
        files = cycle_path.glob(f"{beamline}*.nxs")
        file_runs = ((f, get_run_number(f, beamline)) for f in files)

        # Filter unwanted runs
        if runs_config and runs_config.runs is not None:
            file_runs = ((f, r) for (f, r) in file_runs if r in runs_config.runs)
        file_runs = ((f, r) for (f, r) in file_runs if r not in skip)

        cycle_runs = [RunFile(r, f) for (f, r) in file_runs]

        if cycle_runs:
            available_runs[cycle_dir] = sorted(cycle_runs)
            LOGGER.debug(f"Found {len(cycle_runs)} runs in {cycle_dir}")

    LOGGER.debug(f"Found {len(available_runs)} cycles with runs")
    return available_runs


def make_table_row(cycle_name: str, peak: MonitorPeak):
    """Convert a peak into the format used in the output table

    The order of the fields defines the column order in the table
    """
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


def extract_monitor_peaks(
    archive_mount: str,
    run_mode: RunMode = "incremental",
    runs_config: InstrumentRunConfigs | None = None,
):
    archive = Path(archive_mount)

    for beamline, fit_config in FIT_CONFIGS.items():
        LOGGER.info(f"Finding available runs for '{beamline}'")
        available_runs = find_available_runs_from_archive(
            run_mode, archive, beamline, runs_config[beamline] if runs_config else None
        )

        LOGGER.info(f"Fitting monitor peaks for '{beamline}'")
        for cycle, runs in available_runs.items():
            if not runs:
                continue
            LOGGER.debug(f"Fitting {len(runs)} runs")
            fitted_peaks = (
                fit_monitor_peak(run_file.path, fit_config) for run_file in runs
            )
            peaks = (p for p in fitted_peaks if p is not None)
            rows = [make_table_row(cycle, peak) for peak in peaks]
            if not rows:
                LOGGER.warning("Fitting failed for all runs")
                continue
            yield pa.Table.from_pylist(rows)


def get_run_number(run_file: Path, beamline: str):
    """Extract the run number from a data archive file path.

    e.g. .../PEARL00114302.nxs -> 114302

    Note that this is designed for use on nexus files. The names of non-nexus
    files may include additional characters which cause the function to fail,
    e.g. PEARL00114307_ICPevent.txt
    """
    return int(run_file.stem[len(beamline) :])


class Configuration(BaseSettings):
    archive_mount: str
    run_mode: RunMode = "incremental"
    runs_config: InstrumentRunConfigs | None = None


class Extract(BaseExtract):
    config_cls = Configuration

    def extract_resource_properties(self) -> Iterator[tuple[str, ResourceProperties]]:
        yield (
            "monitor_peaks",
            ResourceProperties(
                extractor=lambda _: extract_monitor_peaks(
                    self.config.archive_mount,
                    self.config.run_mode,
                    self.config.runs_config,
                ),
                write_properties=ResourceWriteProperties(
                    write_mode="merge",
                    merge_on=["beamline", "run_number"],
                    partition={"beamline": "identity", "run_start": "month"},
                ),
            ),
        )
