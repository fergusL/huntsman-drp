""" Code for automated metadata processing of new files """
import time
import queue
import atexit
from contextlib import suppress
from threading import Thread
from astropy import units as u
from astropy.time import Time
from astropy.io import fits

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.datatable import ExposureTable
from huntsman.drp.fitsutil import FitsHeaderTranslator, read_fits_header
from huntsman.drp.utils.library import load_module
from huntsman.drp.quality.utils import recursively_list_fits_files_in_directory
from huntsman.drp.quality.metrics.rawexp import METRICS
from huntsman.drp.quality.utils import screen_success, QUALITY_FLAG_NAME


class Screener(HuntsmanBase):
    """ Class to watch for new file entries in database and process their metadata
    """

    def __init__(self, exposure_table=None, sleep_interval=None, status_interval=60,
                 monitored_directory='/data/nifi/huntsman_priv/images', *args, **kwargs):
        """
        Args:
            sleep_interval (float/int): The amout of time to sleep in between checking for new
                files to screen.
            status_interval (float, optional): Sleep for this long between status reports. Default
                60s.
            monitored_directory (str): The top level directory to watch for new files, so they can
                be added to the relevant datatable.
            *args, **kwargs: Parsed to HuntsmanBase initialiser.
        """
        super().__init__(*args, **kwargs)

        if exposure_table is None:
            self._table = ExposureTable(config=self.config, logger=self.logger)
        self._table = exposure_table

        self._sleep_interval = sleep_interval
        if self._sleep_interval is None:
            self._sleep_interval = 0

        self._status_interval = status_interval

        self._monitored_directory = monitored_directory

        self._n_screened = 0
        self._n_ingested = 0
        self._stop = False
        self._screen_queue = queue.Queue()
        self._ingest_queue = queue.Queue()
        # This might be dumb but need to prevent files being readded to queue
        # before they're been processed
        # TODO figure
        self._files_queued_for_ingest = set()
        self._files_queued_for_screening = set()

        self._ingest_queuer_thread = Thread(target=self._async_ingest_queue_update)
        self._screen_queuer_thread = Thread(target=self._async_screen_queue_update)
        self._ingest_thread = Thread(target=self._async_ingest_files)
        self._screen_thread = Thread(target=self._async_screen_files)
        self._status_thread = Thread(target=self._async_monitor_status)
        self._threads = [self._ingest_queuer_thread, self._screen_queuer_thread,
                         self._ingest_thread, self._screen_thread, self._status_thread]

        atexit.register(self.stop)  # This gets called when python is quit

    @property
    def is_running(self):
        """ Check if the screener is running.
        Returns:
            bool: True if running, else False.
        """
        return self.status["is_running"]

    @property
    def status(self):
        """ Return a status dictionary.
        Returns:
            dict: The status dictionary.
        """
        status = {"is_running": all([t.is_alive() for t in self._threads]),
                  "status_thread": self._status_thread.is_alive(),
                  "ingest_queuer_thread": self._ingest_queuer_thread.is_alive(),
                  "screen_queuer_thread": self._screen_queuer_thread.is_alive(),
                  "ingest_thread": self._ingest_thread.is_alive(),
                  "screen_thread": self._status_thread.is_alive(),
                  "ingest_queued": self._ingest_queue.qsize(),
                  "screen_queued": self._screen_queue.qsize(),
                  "ingested": self._n_ingested,
                  "screened": self._n_screened}
        return status

    def start(self):
        """ Start screening. """
        self.logger.info("Starting screening.")
        self._stop = False
        for thread in self._threads:
            thread.start()

    def stop(self, blocking=True):
        """ Stop screening.
        Args:
            blocking (bool, optional): If True (default), blocks until all threads have joined.
        """
        self.logger.info("Stopping screening.")
        self._stop = True
        if blocking:
            for thread in self._threads:
                with suppress(RuntimeError):
                    thread.join()

    def _async_monitor_status(self):
        """ Report the status on a regular interval. """
        self.logger.debug("Starting status thread.")
        while True:
            if self._stop:
                self.logger.debug("Stopping status thread.")
                break
            # Get the current status
            status = self.status
            self.logger.info(f"screener status: {status}")
            if not self.is_running:
                self.logger.warning(f"screener is not running.")
            # Sleep before reporting status again
            time.sleep(self._status_interval)

    def _async_ingest_queue_update(self):
        """ Watch the data table for unscreened files
        and add all valid files to the screening queue. """
        self.logger.debug("Starting ingest queue updater thread.")
        while True:
            if self._stop:
                self.logger.debug("Stopping ingest queue updater thread.")
                break
            # add files to ingest queue
            for filename in self._get_filenames_to_ingest():
                if filename not in self._files_queued_for_ingest:
                    self._ingest_queue.put([Time.now(), filename])
                    self._files_queued_for_ingest.add(filename)
            # Sleep before checking again
            time.sleep(self._sleep_interval)

    def _async_screen_queue_update(self):
        """ Watch the data table for unscreened files
        and add all valid files to the screening queue. """
        self.logger.debug("Starting screen queue updater thread.")
        while True:
            if self._stop:
                self.logger.debug("Stopping screen queue updater thread.")
                break
            # update list of filenames to screen
            self._get_filenames_to_screen()
            # Loop over filenames and add them to the queue
            # Duplicates are taken care of later on
            for filename in self._get_filenames_to_screen():
                if filename not in self._files_queued_for_screening:
                    self._screen_queue.put([Time.now(), filename])
                    self._files_queued_for_screening.add(filename)
            # Sleep before checking again
            time.sleep(self._sleep_interval)

    def _async_ingest_files(self, sleep=10):
        """ Ingest files in the ingest queue into the datatable.
        Args:
            sleep (float, optional): Sleep for this long while waiting for self.delay_interval to
                expire. Default: 10s.

        #TODO: refactor _async_ingest_files and _async_screen_files into single generic method
        """
        while True:
            if self._stop and self._ingest_queue.empty():
                self.logger.debug("Stopping ingest thread.")
                break
            # Get the oldest file from the queue
            try:
                track_time, filename = self._ingest_queue.get(
                    block=True, timeout=sleep)
            except queue.Empty:
                self.logger.info(f"No new files to process. Sleeping for {self._sleep}s.")
                time.sleep(sleep)
                continue
            try:
                self._ingest_file(filename)
            except Exception as e:
                self.logger.error(f"Something went wrong when trying to ingest {filename}: {e}")
                # remove the file from queue
                self._ingest_queue.task_done()
                continue
            self._n_ingested += 1
            # Tell the queue we are done with this file
            self._ingest_queue.task_done()

    def _async_screen_files(self, sleep=10):
        """ screen files that have been in the queue longer than self.delay_interval.
        Args:
            sleep (float, optional): Sleep for this long while waiting for self.delay_interval to
                expire. Default: 10s.
        """
        while True:
            if self._stop and self._screen_queue.empty():
                self.logger.debug("Stopping screen thread.")
                break
            # Get the oldest file from the queue
            try:
                track_time, filename = self._screen_queue.get(
                    block=True, timeout=sleep)
            except queue.Empty:
                self.logger.info(f"No new files to process. Sleeping for {self._sleep}s.")
                time.sleep(sleep)
                continue
            try:
                self._screen_file(filename)
            except Exception as e:
                self.logger.error(f"Something went wrong when trying to screen {filename}: {e}")
                # remove the file from queue
                self._screen_queue.task_done()
                continue
            self._n_screened += 1
            # Tell the queue we are done with this file
            self._screen_queue.task_done()

    def _get_filenames_to_ingest(self, monitored_directory='/data/nifi/huntsman_priv/images'):
        """ Watch top level directory for new files to process/ingest into database.

        Parameters
        ----------
        monitored_directory : str, optional
            Top level directory to watch for files (including files in subdirectories),
            by default '/data/nifi/huntsman_priv/images'.

        Returns
        -------
        list:
            The list of filenames to process.

        #TODO: monitored_directory should be loaded from a config or somthing
        """
        # create a list of fits files within the directory of interest
        files_in_directory = recursively_list_fits_files_in_directory(self._monitored_directory)
        # list of all entries in data base
        files_in_table = [item['filename'] for item in self._table.find()]
        # determine which files don't have entries in the database and haven't been added to queue
        files_to_ingest = list(
            set(files_in_directory) - set(files_in_table) - self._files_queued_for_ingest)
        return files_to_ingest

    def _get_filenames_to_screen(self):
        """Get valid filenames in the data table to screen

        Returns
        -------
        list
            The list of filenames to screen.
        """
        files_to_screen = []
        # Find any entries in database that haven't been screened
        for document in self._table.find():
            # If file has already been queued ignore it
            if document['filename'] in self._files_queued_for_screening:
                continue
            # if the file represented by this entry hasn't been
            # screened and isn't in the queue, add it to queue
            if not screen_success(document, logger=self.logger):
                # extract fname from entry and append that instead
                files_to_screen.append(document['filename'])

        return files_to_screen

    def _ingest_file(self, filename):
        """Private method that calls the various screening metrics and collates the results.

        Parameters
        ----------
        filename : str
            Filename of image to be ingested.
        """
        # Parse the header before adding to table
        hdr = read_fits_header(filename)
        parsed_header = FitsHeaderTranslator().parse_header(hdr)
        parsed_header["filename"] = filename
        # Create new document in table using parsed header
        self.logger.info(f"Adding quality metadata to database.")
        self._table.insert_one(parsed_header)

    def _screen_file(self, filename):
        """Private method that calls the various screening metrics and collates the results.

        Parameters
        ----------
        filename : str
            Filename of image to be screened.
        """
        metrics = self._get_raw_metrics(filename)

        # Make the document and update the DB
        # TODO: safe to assume there won't be duplicate entries in the datatable?
        # self.logger.info(f'Looking for filename: {filename}')
        # self.logger.info(f'files in table are:\n\n { self._table.find() } \n\n')
        metadata = self._table.find_one({'filename': filename})
        to_update = {"quality": {"rawexp": metrics, "screen_success": True}}
        self._table.update_one(metadata, to_update=to_update)

    def _get_raw_metrics(self, filename):
        """ Evaluate metrics for a raw/unprocessed file.

        Parameters
        ----------
        filename : str
            filename of image to be measured.

        Returns
        -------
        dict
            Dictionary containing the metric values.
        """
        result = {}
        # read the header
        try:
            hdr = read_fits_header(filename)
        except Exception as e:
            self.logger.error(f"Unable to read file header for {filename}: {e}")
            result[QUALITY_FLAG_NAME] = False
            return result
        # get the image data
        try:
            data = fits.getdata(filename)
        except Exception as e:
            self.logger.error(f"Unable to read file {filename}: {e}")
            result[QUALITY_FLAG_NAME] = False
            return result

        for metric in METRICS:
            func = load_module(
                f"huntsman.drp.quality.metrics.rawexp.{metric}")
            result[metric] = func(data, hdr)
        return result
