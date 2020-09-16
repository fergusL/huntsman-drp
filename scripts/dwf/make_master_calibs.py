"""Code to make a master calibs from most recent files for each camera."""
import argparse
from datetime import datetime, timedelta

from huntsman.drp.datatable import RawDataTable
# from huntsman.drp.butler import TemporaryButlerRepository
from huntsman.drp.butler import ButlerRepository


def get_recent_calibs(interval):
    """Get the most recent calibration images."""
    datatable = RawDataTable()
    date_end = datetime.utcnow()
    date_start = datetime.utcnow() - timedelta(days=interval)

    # Get bias filenames
    filenames_bias = datatable.query_column("filename", date_start=date_start, date_end=date_end,
                                            dataType="bias")
    print(f"Found {len(filenames_bias)} bias fields.")

    # Get flat filenames
    # This is a hack to cope with the non-standard field naming
    metalist = datatable.query(date_start=date_start, date_end=date_end)
    filenames_flat = []
    for m in metalist:
        if m["FIELD"].startswith("Flat") and not m["dataType"] == "bias":
            filenames_flat.append(m["filename"])
    print(f"Found {len(filenames_flat)} flat fields.")

    return [*filenames_bias, *filenames_flat]


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', type=int, default=7, help='Time interval in days.')
    args = parser.parse_args()
    interval = args.interval  # Days

    rerun = "dwfrerun"

    # Get filenames
    filenames = get_recent_calibs(interval)

    butler_repo = ButlerRepository("/opt/lsst/software/stack/DATA")

    # Ingest raw data
    butler_repo.ingest_raw_data(filenames, ignore_ingested=True)

    # Make master calibs
    butler_repo.make_master_calibs(calib_date=datetime.utcnow(), rerun=rerun)

    print("Finished.")
