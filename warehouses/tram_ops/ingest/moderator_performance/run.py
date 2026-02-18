"""Fit a Gaussian on the incident monitor to determine the peak centre position.

Currently hardcoded for PEARL.
"""

from dataclasses import dataclass
from pathlib import Path

from mantid.simpleapi import (
    Load,
    NormaliseByCurrent,
    Fit,
    CropWorkspace,
    DeleteWorkspace,
)
import numpy as np


@dataclass
class InstrumentInfo:
    incident_monitor_spectrum: int


@dataclass
class FitResult:
    run_no: int
    uamps: float
    peak_centre: float
    peak_centre_error: float
    peak_intensity: float
    peak_intensity_error: float


INSTRUMENT_INFO = {"PEARL": InstrumentInfo(incident_monitor_spectrum=1)}


def remove_fit_workspaces(fit_ws_prefix: str):
    DeleteWorkspace(f"{fit_ws_prefix}_Parameters")
    DeleteWorkspace(f"{fit_ws_prefix}_Workspace")
    DeleteWorkspace(f"{fit_ws_prefix}_NormalisedCovarianceMatrix")


def run_fit(beamline: str, run_no: int) -> FitResult | None:
    file_hint = f"{beamline.upper()}{run_no}"
    monitor_ws = Load(
        Filename=file_hint,
        SpectrumList=[INSTRUMENT_INFO[beamline].incident_monitor_spectrum],
    )
    run = monitor_ws.getRun()
    pcharge = run.getProtonCharge()
    if pcharge < 1.0:
        DeleteWorkspace(str(monitor_ws))
        return None

    NormaliseByCurrent(InputWorkspace=monitor_ws, OutputWorkspace=monitor_ws)
    monitor_ws = CropWorkspace(
        InputWorkspace=monitor_ws,
        Xmin=1100,
        Xmax=19990,
    )
    # Some constraints included to precent divergence
    fit_ws_prefix = str(monitor_ws) + "_fit"
    fit_output = Fit(
        Function="name=Gaussian,Height=19.2327,\
                    PeakCentre=4843.8,Sigma=1532.64,\
                    constraints=(4600<PeakCentre<5200,1100<Sigma<1900);\
                    name=FlatBackground,A0=16.6099,ties=(A0=16.6099)",
        InputWorkspace=monitor_ws,
        MaxIterations=1000,
        CreateOutput=True,
        Output=fit_ws_prefix,
        OutputCompositeMembers=True,
        StartX=3800,
        EndX=6850,
        Normalise=True,
    )
    paramTable = fit_output.OutputParameters
    #  This catches some fits where the fit constraints are ignored,
    #   allowing the peak to fall far outside the nominal range
    peak_centre = paramTable.column(1)[1]
    if peak_centre > 4600.0 and peak_centre < 5200.0:
        result = FitResult(
            run_no,
            pcharge,
            peak_centre=paramTable.column(1)[1],
            peak_centre_error=paramTable.column(2)[1],
            peak_intensity=paramTable.column(1)[0],
            peak_intensity_error=paramTable.column(2)[0],
        )
    else:
        result = None

    DeleteWorkspace(monitor_ws)
    remove_fit_workspaces(fit_ws_prefix)
    return result
