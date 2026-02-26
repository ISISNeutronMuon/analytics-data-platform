"""Pull raw data from defined beamlines and compute monitor peak positions

It currently requires the ISIS archive to be mounted locally.
"""

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

RunMode = Literal["backfill", "incremental"]

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
        cycle_start="15_2",
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
        cycle_groups = (
            beamline_peaks.sort_by([(c_run_number, "ascending")])
            .group_by("c_cycle")
            .aggregate([(c_cycle, "list")])
        )
        fitted_runs = dict(
            zip(
                cycle_groups[c_cycle].to_pylist(),
                cycle_groups[c_run_number].to_pylist(),
            )
        )
    else:
        logger.debug(f"No runs loaded for '{beamline}'.")

    return fitted_runs


def find_available_runs(
    run_mode: RunMode,
    journals_base_url: str,
    beamline: str,
    cycle_start: str,
    skip: Sequence[int],
) -> Dict[str, Sequence[int]]:
    """Look at journal files to determine the runs that exist.

    If the mode=incremental only look at the most recent cycle.
    """
    logger.debug(
        f"Finding available runs (mode={run_mode}) for {beamline} starting at cycle {cycle_start}"
    )
    journals_beamline_url = f"{journals_base_url}/ndx{beamline.lower()}"
    journal_main = requests.get(journals_beamline_url + "/journal_main.xml")
    journal_main.raise_for_status()

    root = ET.fromstring(journal_main.text)
    journals_cycles = sorted(
        [journalfile.attrib["name"] for journalfile in root.iter("file")], reverse=True
    )
    if run_mode == "incremental":
        journals_cycles = [journals_cycles[0]]

    available_runs = {}
    for filename in journals_cycles:  # reverse iteration
        logger.debug(f"Checking journal {filename}")
        journal_cycle = requests.get(journals_beamline_url + f"/{filename}")
        journal_cycle.raise_for_status()
        root = ET.fromstring(journal_cycle.text)
        namespaces = {
            "journal": root.attrib[
                "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"
            ]
        }
        cycle_runs = [
            int(entry.text.strip())  # type: ignore
            for entry in root.findall(
                ".//journal:NXentry/journal:run_number", namespaces
            )
        ]
        cycle_next_runs = list(filter(lambda run: (run not in skip), cycle_runs))
        cycle_name = f"cycle_{filename[len('journal_') : -len('.xml')]}"
        if len(cycle_next_runs) > 0:
            available_runs[cycle_name] = sorted(cycle_next_runs)

        logger.debug(f"Found {len(cycle_next_runs)} runs.")
        if cycle_start in filename:
            break

    logger.debug(f"Found {len(available_runs)} cycles.")
    return available_runs


def run_file_path(archive_mount: Path, beamline: str, cycle: str, run_no: int) -> Path:
    """Return the path to the given file on the ISIS archive"""
    return (
        archive_mount
        / f"ndx{beamline.lower()}"
        / "Instrument"
        / "data"
        / cycle
        / f"{beamline}{run_no:08}.nxs"
    )


@dlt.resource(write_disposition="append")
def monitor_peaks(
    journals_base_url: str = dlt.config.value,
    archive_mount: str = dlt.config.value,
    run_mode: RunMode = "incremental",
):
    # This defines the column order
    def as_dict(cycle_name: str, peak: MonitorPeak):
        return {
            "beamline": peak.run.beamline,
            "run_number": peak.run.run_number,
            # Use naming convention YYYY/N
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
        available_runs = find_available_runs(
            run_mode,
            journals_base_url,
            beamline,
            fit_config.cycle_start,
            fit_config.skip_runs,
        )
        runs_already_fitted = get_fitted_runs(beamline, pipeline)
        runs_to_fit = {
            key: [
                x
                for x in available_runs[key]
                if x not in set(runs_already_fitted.get(key, []))
            ]
            for key in available_runs
        }
        archive = Path(archive_mount)
        for cycle, runs in runs_to_fit.items():
            logger.debug(f"Fitting runs {runs[0]} -> {runs[-1]}")
            peaks = [
                fit_monitor_peak(
                    run_file_path(archive, beamline, cycle, run_no), fit_config
                )
                for run_no in runs
            ]

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
