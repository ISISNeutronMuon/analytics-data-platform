"""Pull raw data from defined beamlines and compute monitor peak positions

It currently requires the ISIS archive to be mounted locally.
"""

import functools
from pathlib import Path
from typing import Dict, Sequence

import dlt
import elt_common.cli as cli_utils
from elt_common.dlt_destinations.pyiceberg.helpers import load_iceberg_table
from elt_common.dlt_destinations.pyiceberg.pyiceberg_adapter import (
    pyiceberg_adapter,
    PartitionTrBuilder,
)
from fit_monitor import MonitorFitConfig, fit_monitor_peaks, gaussian_plus_flat
import numpy as np
import requests
import xml.etree.ElementTree as ET


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
        #        first_run=90482,
        first_run=125805,
        skip_runs=(95382,),
    )
}


def find_next_runs(
    journals_base_url: str, beamline: str, skip: Sequence[int], after: int
) -> Dict[str, Sequence[int]]:
    """Look at journal files to determine the runs that exist.

    Return a map of cycle id to list of runs
    """
    journals_beamline_url = f"{journals_base_url}/ndx{beamline.lower()}"
    journal_main = requests.get(journals_beamline_url + "/journal_main.xml")
    journal_main.raise_for_status()

    # Assume the likely case is old cycles are loaded. Iterate backwards through the cycles
    # until we hit the range edge
    root = ET.fromstring(journal_main.text)
    journals_cycles = sorted(
        [journalfile.attrib["name"] for journalfile in root.iter("file")], reverse=True
    )
    next_runs = {}
    for filename in journals_cycles:
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
        cycle_next_runs = list(
            filter(lambda run: (run not in skip) and (run > after), cycle_runs)
        )
        cycle_name = f"cycle_{filename[len('journal_') : -len('.xml')]}"
        if len(cycle_next_runs) > 0:
            next_runs[cycle_name] = sorted(cycle_next_runs)

        if len(cycle_next_runs) < len(cycle_runs):
            # We dropped some entries so we must have found the stop marker and don't need to continue
            break

    return next_runs


def run_file_path(archive_mount: Path, beamline: str, cycle: str, run_no: int) -> Path:
    """Return the path to the given file on the ISIS archive"""
    return (
        archive_mount
        / f"ndx{beamline}"
        / "Instrument"
        / "data"
        / cycle
        / f"{beamline}{run_no:08}.nxs"
    )


@dlt.resource(write_disposition="append")
def monitor_peaks(
    journals_base_url: str = dlt.config.value,
    archive_mount: str = dlt.config.value,
):
    # This defines the column order
    def as_dict(peak):
        return {
            "beamline": peak.run.beamline,
            "run_number": peak.run.run_number,
            "run_start": peak.run.start_time,
            "proton_charge": peak.run.proton_charge_uamps,
            "peak_centre": peak.centre,
            "peak_centre_error": peak.centre_error,
            "peak_amplitude": peak.amplitude,
            "peak_amplitude_error": peak.amplitude_error,
            "peak_sigma": peak.sigma,
            "peak_sigma_error": peak.sigma_error,
        }

    from pyiceberg.expressions import EqualTo

    pipeline = dlt.current.pipeline()
    for beamline, fit_config in FIT_CONFIGS.items():
        existing_peaks_table = load_iceberg_table(pipeline, "monitor_peaks")
        latest_run_result = None
        if existing_peaks_table:
            c_beamline, c_run_number = "beamline", "run_number"
            beamline_peaks = existing_peaks_table.scan(
                row_filter=EqualTo(c_beamline, beamline),
                selected_fields=(c_run_number,),
            ).to_arrow()
            if beamline_peaks.num_rows > 0:
                latest_run_result = beamline_peaks.sort_by(
                    [(c_run_number, "descending")]
                ).to_pylist()[0][c_run_number]

        after = (
            latest_run_result
            if latest_run_result is not None
            else fit_config.first_run - 1
        )
        next_runs = find_next_runs(
            journals_base_url, beamline, fit_config.skip_runs, after=after
        )

        archive = Path(archive_mount)
        yield [
            as_dict(peak)
            for peak in fit_monitor_peaks(
                [
                    run_file_path(archive, beamline, cycle, run_no)
                    for cycle, runs in next_runs.items()
                    for run_no in runs
                ],
                FIT_CONFIGS["PEARL"],
            )
        ]


if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="moderator_performance",
        default_destination="elt_common.dlt_destinations.pyiceberg",
        data_generator=pyiceberg_adapter(
            monitor_peaks,
            partition=[
                PartitionTrBuilder.identity("beamline"),
                PartitionTrBuilder.month("run_start"),
            ],
        ),
        dataset_name_suffix="moderator_performance",
    )
