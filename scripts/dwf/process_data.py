"""Script to periodically query and process new data."""
import time
from queue import Queue
from threading import Thread
from datetime import datetime, timedelta

from huntsman.drp.datatable import RawDataTable
from huntsman.drp.bulter import TemporaryButlerRepository


FILTER_NAMES = ["g_band", "r_band", "luminance"]


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
    filenames = datatable.query_column("filename", date_start=time_start, date_end=time_now,
                                       dataType="science")
    return filenames


def process_data_async(queue, filter_names=FILTER_NAMES, make_coadd=False, rerun="dwfrerun"):
    """Get queued filename list and start processing it."""
    while True:
        # Get the next set of filenames
        filenames = queue.get()

        try:
            # Create temp butler repo
            with TemporaryButlerRepository() as butler_repository:

                # Ingest raw data
                butler_repository.ingest_raw_data(filenames)

                # Make calexps
                for filter_name in filter_names:
                    butler_repository.processCcd(dataType="science", rerun=rerun,
                                                 filter_name=filter_name)
                # Assemble coadd
                if make_coadd:
                    butler_repository.make_coadd(rerun=rerun)

        except Exception as err:
            print(f"Error processing files: {err}.")
        finally:
            queue.task_done()


if __name__ == "__main__":

    # Factor these out as command line args
    interval_seconds = 60

    datatable = RawDataTable()
    queue = Queue()

    # Start the queue's worker thread
    thread = Thread(target=process_data_async, daemon=False, args=(queue))

    while True:

        # Get the latest filenames
        filenames = datatable.query_latest(seconds=interval_seconds, column_name="filename")

        # Queue the filenames for processing
        print(f"Queuing {len(filenames)} files.")
        queue.put(filenames)

        # Wait for next batch
        time.sleep(interval_seconds)
