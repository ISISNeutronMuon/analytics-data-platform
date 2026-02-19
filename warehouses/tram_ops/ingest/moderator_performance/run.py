"""Pull raw data from defined beamlines and compute monitor peak positions"""
from typing import Tuple

import dlt
import elt_common.cli as cli_utils
from elt_common.dlt_destinations.pyiceberg.helpers import load_iceberg_table



def run_fits(beamline_name: str, run_range: Tuple[int, int]):


@dlt.resource(write_disposition="append")
def monitor_peaks():
    #pipeline = dlt.current.pipeline()
    #peaks_table = load_iceberg_table(pipeline, "monitor_peaks")



if __name__ == "__main__":
    cli_utils.cli_main(
        pipeline_name="moderator_performance",
        default_destination="elt_common.dlt_destinations.pyiceberg",
        data_generator=monitor_peaks(),
        dataset_name_suffix="moderator_performance",
    )
