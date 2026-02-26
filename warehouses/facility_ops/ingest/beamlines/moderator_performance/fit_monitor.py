"""Provides functions to fit a function on the incident monitor to determine the peak centre position."""

from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Any, Dict, Sequence, Tuple, cast

import h5py
import numpy as np
from scipy.optimize import curve_fit

Range = Tuple[float, float]


@dataclass
class MonitorFitConfig:
    beamline: str
    curve_fit_args: Dict[str, Any]
    cycle_start: str
    skip_runs: Sequence


@dataclass
class Run:
    beamline: str
    run_number: int
    isis_cycle: str
    start_time: dt.datetime
    proton_charge_uamps: float


@dataclass
class FrequencyDistribution:
    x: Any
    y: Any
    ye: Any

    def normalise(self, value: float) -> "FrequencyDistribution":
        self.y /= value
        self.ye /= value
        return self


@dataclass
class Histogram:
    bins: Any
    counts: Any
    errors: Any

    def to_frequencies(self) -> FrequencyDistribution:
        bin_widths = self.bins[1:] - self.bins[:-1]
        x_pts = 0.5 * (self.bins[1:] + self.bins[:-1])
        return FrequencyDistribution(
            x_pts, self.counts / bin_widths, self.errors / bin_widths
        )


@dataclass
class Workspace:
    run_info: Run
    data: Histogram


@dataclass
class MonitorPeak:
    run: Run
    centre: float
    centre_error: float
    amplitude: float
    amplitude_error: float
    sigma: float
    sigma_error: float


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


CURVE_FIT_DEFAULT_ARGS: dict[str, Any] = {
    "maxfev": 1000,
}


def read_monitor(file_path: Path) -> Workspace:
    """Read the spectrum from the first monitor from an ISIS NeXus file.

    Args:
        file_path: Path to the HDF5 file to read.

    Raises:
        KeyError: If any required dataset is not found in the file.
        OSError: If there is an error reading the HDF5 file.
    """

    def _read_dataset(parent: h5py.Group, path: str) -> h5py.Dataset:
        return cast(h5py.Dataset, parent[path])

    try:
        with h5py.File(file_path, "r") as hf:
            raw_data = cast(
                h5py.Group,
                hf["raw_data_1"],
            )
            run_info = Run(
                _read_dataset(raw_data, "name")[0].decode("utf-8"),
                int(_read_dataset(raw_data, "run_number")[0]),
                _read_dataset(raw_data, "isis_cycle")[0].decode("utf-8"),
                dt.datetime.fromisoformat(
                    _read_dataset(raw_data, "start_time")[0].decode("utf-8")
                ),
                float(_read_dataset(raw_data, "proton_charge")[0]),
            )
            counts = np.array(_read_dataset(raw_data, "monitor_1/data")[0, 0, :])
            return Workspace(
                run_info=run_info,
                data=Histogram(
                    np.array(_read_dataset(raw_data, "monitor_1/time_of_flight")[:]),
                    counts,
                    np.sqrt(counts),
                ),
            )

    except KeyError as e:
        raise KeyError(f"Required dataset not found in HDF5 file {file_path}: {e}")
    except OSError as e:
        raise OSError(f"Error reading HDF5 file {file_path}: {e}")


def fit_monitor_peak(
    nxs_file: Path, fit_config: MonitorFitConfig
) -> MonitorPeak | None:
    """Fit a low TOF peak in the given run"""
    monitor_ws = read_monitor(nxs_file)
    if monitor_ws.run_info.run_number in fit_config.skip_runs:
        return None

    pcharge = monitor_ws.run_info.proton_charge_uamps
    if monitor_ws.run_info.proton_charge_uamps < 1.0:
        return None

    freq_data = monitor_ws.data.to_frequencies().normalise(pcharge)
    curve_fit_args = fit_config.curve_fit_args
    fit_range = curve_fit_args["x_range"]
    mask = (freq_data.x >= fit_range[0]) & (freq_data.x <= fit_range[1])
    x, y, ye = freq_data.x[mask], freq_data.y[mask], freq_data.ye[mask]

    try:
        # Fit the data
        popt, pcov = curve_fit(
            curve_fit_args["function"],
            x,
            y,
            sigma=ye,
            absolute_sigma=True,
            p0=curve_fit_args["p0"],
            bounds=curve_fit_args["bounds"],
            **CURVE_FIT_DEFAULT_ARGS,
        )
        # Extract parameters and errors from covariance matrix
        perr = np.sqrt(np.diag(pcov))
        amplitude, peak_centre, sigma = popt
        amplitude_err, peak_centre_err, sigma_err = perr
        return MonitorPeak(
            monitor_ws.run_info,
            float(peak_centre),
            float(peak_centre_err),
            float(amplitude),
            float(amplitude_err),
            float(sigma),
            float(sigma_err),
        )
    except Exception:
        return None
