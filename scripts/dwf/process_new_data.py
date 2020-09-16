"""Script to periodically query and process new data."""
import os
import time
from queue import Queue
from threading import Thread
from datetime import datetime, timedelta

from huntsman.drp.datatable import RawDataTable
from huntsman.drp import lsst_tasks as lsst
from huntsman.drp.bulter import TemporaryButlerRepository


def query_latest_files(datatable, interval):
    """
    Get latest filenames specified by a time interval.

    Args:
        datatable (`huntsman.drp.datatable.RawDataTable`): The raw data table.
        interval (float): The time interval in seconds.

    Returns:
        list of filenames.
    """
    time_now = datetime.utcnow()
    time_start = time_now - timedelta(seconds=interval)
    filenames = datatable.query_column("filename", date_start=time_start, date_end=time_now)
    return filenames


def process_exposures(filenames, calib_date, butler_directory, rerun="dwfrerun", validity=1000,
                      filter="g_band", make_coadd=True):
    """
    Function that takes list of science exposures and processes them to
    produce a coadd. Master calibs are assumed to have already been produced
    amd ingested into butler repo. Skymapper catalogue is also assumed to have
    been ingested.

    Args:
        files (list): List of filepaths for processing.

    TODO:
        -Find way to handle exposures with different filters
    """
    calibdir = os.path.join(butler_directory, "CALIB")

    # Ingest raw exposures
    lsst.ingest_sci_images(filenames, butler_directory=butler_directory, calibdir=calibdir)

    # Ingest the master calibs
    lsst.ingest_master_bias(calib_date, butler_directory=butler_directory, calibdir=calibdir,
                            rerun=rerun, validity=validity)
    lsst.ingest_master_flat(calib_date, filter, butler_directory=butler_directory,
                            calibdir=calibdir, rerun=rerun, validity=validity)

    # Create calibrated exposures
    lsst.processCcd(dataType='science', butler_directory=butler_directory, calibdir=calibdir,
                    rerun=rerun)

    # Make the coadd
    if make_coadd:
        lsst.makeDiscreteSkyMap(butler_directory=butler_directory, rerun=f'{rerun}:coadd')
        lsst.makeCoaddTempExp(filter, butler_directory=butler_directory, calibdir=calibdir,
                              rerun=f'{rerun}:coadd')
        lsst.assembleCoadd(filter, butler_directory=butler_directory, calibdir=calibdir,
                           rerun=f'{rerun}:coadd')


def process_data_async(queue):
    """Get queued filename list and start processing it."""
    while True:
        date = datetime.utcnow().strftime('%Y-%m-%d')
        filenames = queue.get()

        # Create temp butler repo
        butler_repo = TemporaryButlerRepository()
        with butler_repo:
            process_exposures(filenames, butler_directory=butler_repo.butlerdir,
                              calib_date=date)
        queue.task_done()


if __name__ == "__main__":

    # Factor these out as command line args
    interval = 60

    datatable = RawDataTable()
    queue = Queue()

    # Start the queue's worker thread
    thread = Thread(target=process_data_async, daemon=False, args=(queue))

    while True:

        # Get the latest filenames
        filenames = query_latest_files(datatable, interval)

        # Queue the filenames for processing
        print(f"Queuing {len(filenames)} files.")
        queue.put(filenames)

        # Wait for next batch
        time.sleep(interval)
