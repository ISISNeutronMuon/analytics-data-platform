"""Pull raw data from defined beamlines and compute monitor peak positions

It currently requires the ISIS archive to be mounted locally.
"""

import functools
from pathlib import Path

import dlt
import elt_common.cli as cli_utils
from elt_common.dlt_destinations.pyiceberg.helpers import load_iceberg_table
from fit_monitor import MonitorFitConfig, fit_monitor_peaks, gaussian_plus_flat
import numpy as np

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
        first_run=90482,
        skip_runs=(95382,),
    )
}


def run_file_path(archive_mount: Path, beamline: str, cycle: str, run_no: int) -> Path:
    return (
        archive_mount
        / f"ndx{beamline}"
        / "Instrument"
        / "data"
        / cycle
        / f"{beamline}{run_no:08}.nxs"
    )


@dlt.resource(write_disposition="append")
def monitor_peaks(archive_mount: str = dlt.config.value):
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

    archive = Path(archive_mount)
    yield [
        as_dict(peak)
        for peak in fit_monitor_peaks(
            [run_file_path(archive, "PEARL", "cycle_15_2", 90482)], FIT_CONFIGS["PEARL"]
        )
    ]


if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="moderator_performance",
        default_destination="elt_common.dlt_destinations.pyiceberg",
        data_generator=monitor_peaks(),
        dataset_name_suffix="moderator_performance",
    )
