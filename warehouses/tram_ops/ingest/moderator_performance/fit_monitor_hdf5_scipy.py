"""Fit a Gaussian on the incident monitor to determine the peak centre position.

Currently hardcoded for PEARL.
"""

from dataclasses import dataclass
import functools
from pathlib import Path
from typing import Any, Dict, Sequence, Tuple

import h5py
import numpy as np
from numpy.typing import ArrayLike
from scipy.optimize import curve_fit

Range = Tuple[float, float]


@dataclass
class BeamlineInfo:
    incident_monitor_spectrum: int
    fit_args: Dict[str, Any]
    peak_centre_accepted_range: Range
    first_run: int
    skip_runs: Sequence


@dataclass
class RunInfo:
    beamline: str
    run_number: int
    start_time: np.datetime64
    proton_charge_uamps: float


@dataclass
class Workspace:
    y: ArrayLike
    x: ArrayLike
    err: ArrayLike
    run_info: RunInfo


@dataclass
class MonitorPeak:
    run: RunInfo
    centre: float
    centre_error: float
    intensity: float
    intensity_error: float

    def as_dict(self):
        return {
            "beamline": self.run.beamline,
            "run_number": self.run.run_number,
            "run_start": np.datetime_as_string(
                self.run.start_time, unit="s", timezone="UTC"
            ),
            "proton_charge": self.run.proton_charge_uamps,
            "peak_centre": self.centre,
            "peak_centre_error": self.centre_error,
            "peak_intensity": self.intensity,
            "peak_intensity_error": self.intensity_error,
        }


BEAMLINE_INFO = {
    "PEARL": BeamlineInfo(
        incident_monitor_spectrum=1,
        fit_args={
            "Function": "name=Gaussian,Height=19.2327,\
                    PeakCentre=4843.8,Sigma=1532.64,\
                    constraints=(4600<PeakCentre<5200,1100<Sigma<1900);\
                    name=FlatBackground,A0=16.6099,ties=(A0=16.6099)",
            "StartX": 3800,
            "EndX": 6850,
        },
        peak_centre_accepted_range=(4600.0, 5200.0),
        first_run=90482,
        skip_runs=(95382,),
    )
}
FIT_DEFAULT_ARGS = {
    "MaxIterations": 1000,
    "CreateOutput": True,
    "OutputCompositeMembers": True,
    "Normalise": True,
}


def read_monitor_data(file_path: str | Path) -> Workspace:
    """Read the first monitor data from an ISIS NeXus file.

    Args:
        file_path: Path to the HDF5 file to read.

    Returns:
        A dictionary with keys:
            - 'proton_charge': Scalar float
            - 'monitor_data': 1D numpy array of floats
            - 'time_of_flight': 1D numpy array of floats

    Raises:
        FileNotFoundError: If the file does not exist.
        KeyError: If any required dataset is not found in the file.
        OSError: If there is an error reading the HDF5 file.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"HDF5 file not found: {file_path}")

    try:
        with h5py.File(file_path, "r") as hf:
            raw_data = hf["/raw_data_1"]
            run_info = RunInfo(
                raw_data["name"][0].decode("utf-8"),  # type: ignore
                int(raw_data["run_number"][0]),  # type: ignore
                str(raw_data["start_time"][0]),  # type: ignore
                float(raw_data["proton_charge"][0]),  # type: ignore
            )
            counts = np.array(raw_data["monitor_1/data"][0, 0, :])  # type: ignore
            return Workspace(
                y=counts,
                x=np.array(raw_data["monitor_1/time_of_flight"][:]),  # type: ignore
                err=np.sqrt(counts),
                run_info=run_info,
            )

    except KeyError as e:
        raise KeyError(f"Required dataset not found in HDF5 file {file_path}: {e}")
    except OSError as e:
        raise OSError(f"Error reading HDF5 file {file_path}: {e}")


def fit_monitor_peak_hdf_scipy(beamline: str, beamline_info: BeamlineInfo, run_no: int):
    monitor_ws = read_monitor_data(
        "/home/ubuntu/mnt/archive/NDXPEARL/Instrument/data/cycle_15_2/PEARL00090482.nxs"
    )
    if monitor_ws.run_info.proton_charge_uamps < 1.0:
        return None

    # Normalize and convert to frequency data
    x_points = 0.5 * (monitor_ws.x[1:] + monitor_ws.x[:-1])
    x_bin_widths = monitor_ws.x[1:] - monitor_ws.x[:-1]
    print(monitor_ws.y.shape)
    signal_freq = monitor_ws.y / monitor_ws.run_info.proton_charge_uamps / x_bin_widths
    signal_err = monitor_ws.err / monitor_ws.run_info.proton_charge_uamps / x_bin_widths

    fit_start = beamline_info.fit_args["StartX"]
    fit_end = beamline_info.fit_args["EndX"]
    mask = (x_points >= fit_start) & (x_points <= fit_end)
    x_fit = x_points[mask]
    y_fit = signal_freq[mask]
    yerr_fit = signal_err[mask]

    def gaussian_plus_flat(
        x,
        amplitude,
        mu,
        sigma,
        constant,
    ):
        """Gaussian peak plus flat background."""
        gaussian = amplitude * np.exp(-0.5 * ((x - mu) ** 2) / (sigma**2))
        return gaussian + constant

    # Initial parameter guesses
    p0 = [
        19.2327,  # amplitude
        4843.8,  # mu (peak centre)
        1532.64,  # sigma
    ]

    # Fit the data
    popt, pcov = curve_fit(
        functools.partial(gaussian_plus_flat, constant=16.6099),
        x_fit,
        y_fit,
        sigma=yerr_fit,
        absolute_sigma=True,
        p0=p0,
        bounds=(
            (-np.inf, 4600, 1100),
            (np.inf, 5200, 1900),
        ),
        maxfev=1000,
    )

    # Extract parameters and errors from covariance matrix
    perr = np.sqrt(np.diag(pcov))
    amplitude, mu, _ = popt
    amplitude_err, mu_err, _ = perr

    peak_centre = mu

    # Check if peak is within accepted range
    if (
        peak_centre > beamline_info.peak_centre_accepted_range[0]
        and peak_centre < beamline_info.peak_centre_accepted_range[1]
    ):
        result = MonitorPeak(
            monitor_ws.run_info,
            centre=peak_centre,
            centre_error=mu_err,
            intensity=amplitude,
            intensity_error=amplitude_err,
        )
    else:
        result = None

    return result


def fit_monitor_peaks(
    beamline: str,
    run_start: int,
    run_end: int,
):
    """Fit all monitor peaks for the run range for the given beamline.

    If a filename is given the result is saved as a JSON file."""
    beamline_info = BEAMLINE_INFO[beamline]
    monitor_peaks = []
    for run_no in range(run_start, run_end + 1):
        if run_no in beamline_info.skip_runs:
            continue

        monitor_peak = fit_monitor_peak_hdf_scipy(beamline, beamline_info, run_no)
        if monitor_peak:
            monitor_peaks.append(monitor_peak)

    return monitor_peaks


import pprint

# print()
pprint.pprint(fit_monitor_peaks("PEARL", run_start=90482, run_end=90482))
