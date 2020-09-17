"""Code to make a master calibs from most recent files for each camera."""
import argparse

from huntsman.drp.utils import current_date
from huntsman.drp.datatable import RawDataTable
from huntsman.drp.butler import ButlerRepository


def get_recent_calibs(interval_days, **kwargs):
    """Get the most recent calibration images."""

    datatable = RawDataTable(0)

    # Get bias filenames
    filenames_bias = datatable.query_latest(days=interval_days, dataType="bias",
                                            column_name="filename", **kwargs)
    print(f"Found {len(filenames_bias)} bias fields.")

    # Get flat filenames
    """
    filenames_flat = datatable.query_latest(days=interval_days, dataType="flat",
                                            column_name="filename", **kwargs)
    """
    # This is a hack to cope with the non-standard field naming
    metalist = datatable.query_latest(days=interval_days, **kwargs)
    filenames_flat = []
    for m in metalist:
        if m["FIELD"].startswith("Flat") and not m["dataType"] == "bias":
            filenames_flat.append(m["filename"])
    print(f"Found {len(filenames_flat)} flat fields.")

    return [*filenames_bias, *filenames_flat]


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', type=int, default=1, help='Time interval in days.')
    args = parser.parse_args()
    interval_days = args.interval  # Days

    rerun = "dwfrerun"

    # Get filenames
    filenames = get_recent_calibs(interval_days, ccd=2)

    # Make butler repository
    with ButlerRepository("/opt/lsst/software/stack/DATA") as butler_repo:

        # Ingest raw data
        butler_repo.ingest_raw_data(filenames, ignore_ingested=True)

        # Make master calibs
        butler_repo.make_master_calibs(calib_date=current_date(), rerun=rerun, skip_bias=False)

    print("Finished.")
