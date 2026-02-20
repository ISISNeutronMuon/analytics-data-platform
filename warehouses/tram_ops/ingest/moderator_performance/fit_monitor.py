"""Fit a Gaussian on the incident monitor to determine the peak centre position.

Currently hardcoded for PEARL.
"""

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, Sequence, Tuple

from mantid.simpleapi import (
    Load,
    NormaliseByCurrent,
    Fit,
    DeleteWorkspace,
)
import numpy as np

Range = Tuple[float, float]


@dataclass
class BeamlineInfo:
    incident_monitor_spectrum: int
    crop_x: Range
    fit_args: Dict[str, Any]
    peak_centre_accepted_range: Range
    first_run: int
    skip_runs: Sequence


@dataclass
class Run:
    beamline: str
    number: int
    start_time: np.datetime64
    uamps: float


@dataclass
class MonitorPeak:
    run: Run
    centre: float
    centre_error: float
    intensity: float
    intensity_error: float

    def as_dict(self):
        return {
            "beamline": self.run.beamline,
            "run_number": self.run.number,
            "run_start": np.datetime_as_string(
                self.run.start_time, unit="s", timezone="UTC"
            ),
            "proton_charge": self.run.uamps,
            "peak_centre": self.centre,
            "peak_centre_error": self.centre_error,
            "peak_intensity": self.intensity,
            "peak_intensity_error": self.intensity_error,
        }


def save_json(filename: Path, monitor_peaks: Sequence[MonitorPeak]):
    """Dump the peaks information as a.json file"""
    # Convert to dict first and if this fails it doesn't write and empty file
    peaks = [peak.as_dict() for peak in monitor_peaks]
    with open(filename, "w") as fp:
        json.dump(peaks, fp)


BEAMLINE_INFO = {
    "PEARL": BeamlineInfo(
        incident_monitor_spectrum=1,
        crop_x=(1100, 19990),
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


def remove_fit_workspaces(fit_ws_prefix: str):
    """Clean up workspaces as a result of Fit algorithm"""
    DeleteWorkspace(f"{fit_ws_prefix}_Parameters")
    DeleteWorkspace(f"{fit_ws_prefix}_Workspace")
    DeleteWorkspace(f"{fit_ws_prefix}_NormalisedCovarianceMatrix")


def fit_monitor_peak(
    beamline: str, beamline_info: BeamlineInfo, run_no: int
) -> MonitorPeak | None:
    """Fit the peak in monitor spectrum"""
    file_hint = f"{beamline.upper()}{run_no}"
    monitor_ws = Load(
        Filename=file_hint,
        SpectrumList=[beamline_info.incident_monitor_spectrum],
    )
    run = monitor_ws.getRun()
    pcharge = run.getProtonCharge()
    if pcharge < 1.0:
        DeleteWorkspace(str(monitor_ws))
        return None

    NormaliseByCurrent(InputWorkspace=monitor_ws, OutputWorkspace=monitor_ws)

    # Some constraints included to prevent divergence
    fit_ws_prefix = str(monitor_ws) + "_fit"
    fit_output = Fit(
        InputWorkspace=monitor_ws,
        Output=fit_ws_prefix,
        **FIT_DEFAULT_ARGS,
        **beamline_info.fit_args,
    )
    paramTable = fit_output.OutputParameters
    for i in range(paramTable.rowCount()):
        print(paramTable.row(i))
    #  This catches some fits where the fit constraints are ignored,
    #   allowing the peak to fall far outside the nominal range
    peak_centre = paramTable.column(1)[1]
    if (
        peak_centre > beamline_info.peak_centre_accepted_range[0]
        and peak_centre < beamline_info.peak_centre_accepted_range[1]
    ):
        run = Run(beamline, run_no, run.startTime().to_datetime64(), pcharge)
        result = MonitorPeak(
            run,
            centre=paramTable.column(1)[1],
            centre_error=paramTable.column(2)[1],
            intensity=paramTable.column(1)[0],
            intensity_error=paramTable.column(2)[0],
        )
    else:
        result = None

    DeleteWorkspace(monitor_ws)
    remove_fit_workspaces(fit_ws_prefix)
    return result


def fit_monitor_peaks(
    beamline: str, run_start: int, run_end: int, json_filename: Path | None = None
) -> Sequence[MonitorPeak]:
    """Fit all monitor peaks for the run range for the given beamline.

    If a filename is given the result is saved as a JSON file."""
    beamline_info = BEAMLINE_INFO[beamline]
    monitor_peaks = []
    for run_no in range(run_start, run_end + 1):
        if run_no in beamline_info.skip_runs:
            continue

        monitor_peak = fit_monitor_peak(beamline, beamline_info, run_no)
        if monitor_peak:
            monitor_peaks.append(monitor_peak)

    if json_filename is not None:
        save_json(json_filename, monitor_peaks)

    return monitor_peaks


print(fit_monitor_peaks("PEARL", 90482, 90482))
