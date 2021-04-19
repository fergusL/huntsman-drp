""" Continually produce, update and archive master calibs. """
import time
import datetime
from threading import Thread

from panoptes.utils.time import CountdownTimer

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import date_to_ymd, parse_date
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection
from huntsman.drp.lsst.butler import TemporaryButlerRepository
from huntsman.drp.document import CalibDocument
from huntsman.drp.utils.calib import get_calib_filename


class MasterCalibMaker(HuntsmanBase):

    _date_key = "dateObs"

    def __init__(self, exposure_table=None, calib_table=None, nproc=1, **kwargs):
        super().__init__(**kwargs)

        self._calib_types = self.config["calibs"]["types"]
        self._nproc = int(nproc)

        validity = self.config["calibs"]["validity"]
        self._validity = datetime.timedelta(days=validity)  # TODO: Validity based on calib type

        # Create datatable objects
        if exposure_table is None:
            exposure_table = RawExposureCollection(config=self.config, logger=self.logger)
        self._exposure_table = exposure_table

        if calib_table is None:
            calib_table = MasterCalibCollection(config=self.config, logger=self.logger)
        self._calib_table = calib_table

        # Create threads
        self._stop_threads = False
        self._calib_thread = Thread(target=self._run)

    # Properties

    @property
    def is_running(self):
        """ Check if the asynchronous calib processing loop is running.
        Returns:
            bool: True if running, else False.
        """
        return self._calib_thread.is_alive()

    # Public methods

    def start(self):
        """ Start the asynchronous calib processing loop. """
        self.logger.info("Starting master calib maker.")
        self._stop_threads = False
        self._calib_thread.start()

    def stop(self):
        """ Stop the asynchronous calib processing loop.
        Note that this will block until any ongoing processing has finished.
        """
        self.logger.info("Stopping master calib maker.")
        self._stop_threads = True
        try:
            self._calib_thread.join()
            self.logger.info("Calib maker stopped.")
        except RuntimeError:
            pass

    def process_date(self, calib_date):
        """ Update all master calibs for a given calib date.
        Args:
            calib_date (object): The calib date.
        """
        # Get metadata for all raw calibs that are valid for this date
        raw_data_ids = self._find_raw_calibs(calib_date=calib_date)

        # Get a list of all unique calib IDs from the raw calibs
        calib_ids_all = self._get_unique_calib_ids(calib_date=calib_date, documents=raw_data_ids)
        self.logger.info(f"Found {len(calib_ids_all)} unique calib IDs for"
                         f" calib_date={calib_date}.")

        # Figure out which calib IDs need processing
        calibs_to_process = [c for c in calib_ids_all if self._should_process(c, raw_data_ids)]
        self.logger.info(f"{len(calibs_to_process)} calib IDs require processing for"
                         f" calib_date={calib_date}.")

        if not calibs_to_process:
            self.logger.warning(f"No calibIds require processing for calibDate={calib_date}.")
            return

        # Identify existing calibs that need ingesting
        calibs_to_ingest = [c for c in calib_ids_all if c not in calibs_to_process]

        # Figure out if we can skip any of the calibs
        datasetTypes_to_skip = []

        if not any([_["datasetType"] == "bias" for _ in calibs_to_process]):
            datasetTypes_to_skip.append("bias")

            # Only skip darks if we are also skipping biases
            if not any([_["datasetType"] == "dark" for _ in calibs_to_process]):
                datasetTypes_to_skip.append("dark")

        # Process data in a temporary butler repo
        with TemporaryButlerRepository(calib_table=self._calib_table) as br:

            # Ingest raw exposures
            br.ingest_raw_data([_["filename"] for _ in raw_data_ids])

            # Ingest any existing master calibs
            for calib_type in self._calib_types:

                filenames = [
                    c["filename"] for c in calibs_to_ingest if c["calibType"] == calib_type]
                if filenames:
                    br.ingest_master_calibs(calib_type, filenames=filenames,
                                            validity=self._validity)

                # Check if there is sufficient data to proceed
                elif calib_type in datasetTypes_to_skip:
                    self.logger.warning(f"No {calib_type} frames available for {calib_date}."
                                        " Skipping.")
                    return

            # Make master calibs without raising errors
            br.make_master_calibs(calib_date=calib_date, datasetTypes_to_skip=datasetTypes_to_skip,
                                  validity=self._validity.days, procs=self._nproc)

            # Archive the master calibs
            try:
                self.logger.info(f"Archiving master calibs for calib_date={calib_date}.")
                br.archive_master_calibs()

            except Exception as err:
                self.logger.warning(f"Unable to archive master calibs for calib_date={calib_date}:"
                                    f" {err!r}")

    # Private methods

    def _run(self, sleep=300):
        """ Continually call self.process_date for each unique calib date.
        Args:
            sleep (float, optional): Sleep for this long between restarts.
        """
        while True:

            calib_dates = self._get_unique_dates()
            self.logger.info(f"Found {len(calib_dates)} unique calib dates.")

            for calib_date in calib_dates:

                if self._stop_threads:
                    return

                self.logger.info(f"Processing calibs for calib_date={calib_date}.")
                self.process_date(calib_date)

            self.logger.info(f"Finished processing calib dates. Sleeping for {sleep} seconds.")
            timer = CountdownTimer(duration=sleep)
            while not timer.expired():
                if self._stop_threads:
                    return
                time.sleep(1)

    def _should_process(self, calib_id, raw_data_ids):
        """ Check if the given calib_id should be processed based on existing raw data.
        Args:
            calib_id (CalibDocument): The calib ID.
            raw_data_ids (list of DataId): The raw exposure dataIDs.
        Returns:
            bool: True if the calib ID requires processing, else False.
        """
        query_result = self._calib_table.find_one(document_filter=calib_id)

        # If the calib does not already exist, return True
        if not query_result:
            return True

        if len(query_result) > 1:
            raise RuntimeError(f"calib_id {calib_id} matched with multiple documents.")

        # If there are new files for this calib, return True
        if any([r["date_modified"] >= query_result["date_modified"] for r in raw_data_ids]):
            return True

        return False

    def _find_raw_calibs(self, calib_date):
        """ Find all valid raw calibs in the raw exposure collection given a calib date.
        Args:
            calib_date (object): The calib date.
        Returns:
            list of RawExposureDocument: The documents.
        """
        parsed_date = parse_date(calib_date)
        date_start = parsed_date - self._validity
        date_end = parsed_date + self._validity

        docs = []
        for calib_type in self._calib_types:

            docs_of_type = self._exposure_table.find(
                {"dataType": calib_type}, date_start=date_start, date_end=date_end, screen=True,
                quality_filter=True)

            self.logger.info(f"Found {len(docs_of_type)} raw {calib_type} calibs for"
                             f" calib_date={calib_date}.")

            docs.extend(docs_of_type)

        return docs

    def _get_unique_calib_ids(self, calib_date, documents):
        """ Get all possible CalibDocuments from a set of documents.
        Args:
            calib_date (object): The calib date.
            documents (list): The list of documents.
        Returns:
            list of CalibDocument: The calb documents.
        """
        calib_date = date_to_ymd(calib_date)
        calib_types = self.config["calibs"]["types"]

        unique_calib_ids = []

        for document in documents:

            calib_type = document["dataType"]
            if calib_type not in calib_types:
                continue

            keys = self.config["calibs"]["matching_columns"][calib_type]
            calib_dict = {k: document[k] for k in keys}
            calib_dict["calibDate"] = calib_date
            calib_dict["datasetType"] = calib_type

            # Get the filename of the archived calib
            filename = get_calib_filename(config=self.config, **calib_dict)
            calib_dict["filename"] = filename

            calib_id = CalibDocument(calib_dict)
            if calib_id not in unique_calib_ids:
                unique_calib_ids.append(calib_id)

        return unique_calib_ids

    def _get_unique_dates(self):
        """ Get all calib dates specified by files in the raw data table.
        Returns:
            list of datetime: The list of dates.
        """
        dates = set(
            [date_to_ymd(d) for d in self._exposure_table.find(key="date", screen=True,
             quality_filter=True)])
        return list(dates)
