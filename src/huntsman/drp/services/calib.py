""" Continually produce, update and archive master calibs. """
import os
import time
import datetime
from threading import Thread

from panoptes.utils.time import CountdownTimer

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import date_to_ymd
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection
from huntsman.drp.lsst.butler import TemporaryButlerRepository


class MasterCalibMaker(HuntsmanBase):

    _date_key = "dateObs"

    def __init__(self, exposure_collection=None, calib_collection=None, nproc=None, **kwargs):
        """
        Args:
            nproc (int): The number of processes to use. If None (default), will check the config
                item `calib-maker.nproc` with a default value of 1.
        """
        super().__init__(**kwargs)

        self._ordered_calib_types = self.config["calibs"]["types"]
        self._validity = datetime.timedelta(days=self.config["calibs"]["validity"])

        calib_maker_config = self.config.get("calib-maker", {})
        self._min_docs_per_calib = calib_maker_config.get("min_docs_per_calib", 1)
        self._max_docs_per_calib = calib_maker_config.get("max_docs_per_calib", None)
        self._nproc = int(nproc if nproc else calib_maker_config.get("nproc", 1))

        # Create collection client objects
        if exposure_collection is None:
            exposure_collection = RawExposureCollection(config=self.config, logger=self.logger)
        self._exposure_collection = exposure_collection

        if calib_collection is None:
            calib_collection = MasterCalibCollection(config=self.config, logger=self.logger)
        self._calib_collection = calib_collection

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
        # Get set of all unique calib IDs from the raw calibs
        calib_docs = self._exposure_collection.get_calib_docs(calib_date=calib_date,
                                                              validity=self._validity)
        # Figure out which calib IDs need processing and which ones we can ingest
        calibs_to_process, calibs_to_ingest = self._get_calib_sets(calib_docs)
        if not calibs_to_process:
            self.logger.warning(f"No calibIds require processing for calibDate={calib_date}.")
            return

        # Get set of raw calibs that need processing
        raw_docs_to_process = set()
        for calib_doc in calibs_to_process:
            raw_docs_to_process.update(self._get_matching_raw_docs(calib_doc))

        self.logger.info(f"Raw documents to process: {len(raw_docs_to_process)}.")

        self._process_documents(raw_docs_to_process, calibs_to_process, calibs_to_ingest)

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

    def _should_process(self, calib_doc, ignore_date=False):
        """ Check if the given calib_doc should be processed based on existing raw data.
        Args:
            calib_doc (CalibDocument): The calib document.
            ignore_date (bool, optional): If True, ignore the date when deciding if the document
                should be processed. Default False.
        Returns:
            bool: True if the calib ID requires processing, else False.
        """
        raw_docs = self._get_matching_raw_docs(calib_doc)

        # If there aren't enough matches then return False
        if self._min_docs_per_calib:
            if len(raw_docs) < self._min_docs_per_calib:
                self.logger.debug(f"Not enough raw exposures to make calibId={calib_doc}.")
                return False

        # If the calib does not already exist, we need to make it
        if not self._calib_collection.find_one(document_filter=calib_doc):
            return True

        # If the file doesn't exist, we need to make it
        if not os.path.isfile(calib_doc["filename"]):
            return True

        # If there are new files for this calib, we need to make it again
        if not ignore_date:
            if any([r["date_modified"] >= calib_doc["date_modified"] for r in raw_docs]):
                return True

        # If there are no new files contributing to this existing calib, we can skip it
        return False

    def _get_calib_sets(self, calib_docs):
        """ Identify which calib IDs need processing and which should be ingested.
        Args:
            calib_docs (list of CalibDocument): The list of calib docs to check.
        Returns:
            Set of CalibDocument: Calib documents that need to be processed.
            Set of CalibDocument: Calib documents that need to be ingested.
        """
        calibs_to_process = set()
        calibs_to_ingest = set()

        for calib_doc in calib_docs:
            if calib_doc in calibs_to_process:
                continue

            # Check if we should process this calib
            if self._should_process(calib_doc):

                # Find all dependent calibs that need to be recreated
                calib_docs_dep = [d for d in self._get_dependent_calibs(calib_doc) if
                                  self._should_process(d, ignore_date=True)]

                # Update the sets
                calibs_to_process.update(calib_docs_dep)
                calibs_to_ingest.difference_update(calib_docs_dep)

            # If we don't need to remake the calib then we should ingest it
            else:
                calibs_to_ingest.add(calib_doc)

        self.logger.info(f"Calibs to process: {len(calibs_to_process)},"
                         f" calibs to ingest: {len(calibs_to_ingest)}.")

        return calibs_to_process, calibs_to_ingest

    def _get_matching_raw_docs(self, calib_doc):
        """ Get matchig raw exposure docs for a particular calib.
        Args:
            calib_doc (CalibDocument): The calib document.
        Returns:
            list of RawExposureDocument: The matching raw exposure documents.
        """
        calib_date = calib_doc["calibDate"]
        docs = self._exposure_collection.get_matching_raw_calibs(calib_doc, calib_date=calib_date,
                                                                 validity=self._validity)

        if self._max_docs_per_calib:

            if len(docs) > self._max_docs_per_calib:
                self.logger.warning(f"Limiting to {self._max_docs_per_calib} docs for"
                                    f" calibId={calib_doc}.")

                # Docs already ordered by increasing time difference
                docs = docs[:self._max_docs_per_calib]

        return docs

    def _get_unique_dates(self):
        """ Get all calib dates specified by files in the raw data table.
        Returns:
            set of datetime: The list of dates.
        """
        dates = self._exposure_collection.find(key="date", screen=True, quality_filter=True)
        return set([date_to_ymd(d) for d in dates])

    def _get_dependent_calibs(self, calib_doc):
        """ Get all dependent calibs for a calib doc.
        For example, a flat depends on the dark and bias used to create it.
        Args:
            calib_doc (CalibDocument): The calib document.
        Returns:
            list of CalibDoc: All dependent calibs, including the input calib doc.
        """
        calib_docs = set([calib_doc])
        dataset_type = calib_doc["datasetType"]

        # Identify columns used to identify dependent calibs
        matching_columns = self.config["calibs"]["matching_columns"][dataset_type].copy()
        if "calibDate" not in matching_columns:
            matching_columns.append("calibDate")

        # Make the matching dict
        matching_dict = {k: calib_doc[k] for k in matching_columns}

        if dataset_type == "flat":  # Nothing to be done here
            pass

        # If dataset type is dark, need to add all dependent flats
        if dataset_type in ("bias", "dark"):
            query = matching_dict.copy()
            query["datasetType"] = "flat"
            new_docs = self._calib_collection.find(query)
            if new_docs is not None:
                calib_docs.update(new_docs)

        # If dataset type is dark, need to add all dependent biases and darks
        if dataset_type == "bias":
            query = matching_dict.copy()
            query["datasetType"] = {"in": ["dark", "flat"]}
            new_docs = self._calib_collection.find(query)
            if new_docs is not None:
                calib_docs.update(new_docs)

        return calib_docs

    def _process_documents(self, raw_docs_to_process, calibs_to_process, calibs_to_ingest):
        """ Make the new master calibs using the LSST code stack.
        Args:
            raw_docs_to_process (set of RawExposureDocument): Documents to use to create calibs.
            calibs_to_process (set of CalibDocument): Calib documents to create.
            calibs_to_ingest (set of calibDocument): Calib documents to ingest.
        """
        # Process data in a temporary butler repo
        with TemporaryButlerRepository() as br:

            # Ingest raw exposures
            br.ingest_raw_data([_["filename"] for _ in raw_docs_to_process])

            # Ingest existing master calibs
            for calib_type in self._ordered_calib_types:
                fns = [c["filename"] for c in calibs_to_ingest if c["datasetType"] == calib_type]
                br.ingest_master_calibs(calib_type, filenames=fns, validity=self._validity.days)

            # Make master calibs
            # NOTE: Implicit error handling
            calib_docs = br.make_master_calibs(
                calib_docs=calibs_to_process, validity=self._validity.days, procs=self._nproc)

            # Archive the master calibs
            for calib_doc in calib_docs:
                self._calib_collection.archive_master_calib(filename=calib_doc["filename"],
                                                            metadata=calib_doc)
