""" Continually produce, update and archive master calibs. """
import os
import datetime
from threading import Thread

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import date_to_ymd, parse_date
from huntsman.drp.datatable import ExposureTable, MasterCalibTable
from huntsman.drp.butler import TemporaryButlerRepository
from huntsman.drp.dataid import CalibId
from huntsman.drp.utils.calib import get_calib_filename


class MasterCalibMaker(HuntsmanBase):

    _date_key = "dateObs"

    def __init__(self, exposure_table=None, calib_table=None, **kwargs):
        super().__init__(**kwargs)

        self._calib_types = self.config["calibs"]["types"]

        validity = self.config["calibs"]["validity"]
        self._validity = datetime.timedelta(days=validity)  # TODO: Validity based on calib type

        # Create datatable objects
        if exposure_table is None:
            exposure_table = ExposureTable(config=self.config, logger=self.logger)
        self._exposure_table = exposure_table

        if calib_table is None:
            calib_table = MasterCalibTable(config=self.config, logger=self.logger)
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
        calib_ids = self._get_unique_calib_ids(calib_date=calib_date, documents=raw_data_ids)

        self.logger.info(f"Found {len(calib_ids)} unique calib IDs for calib_date={calib_date}.")

        # Figure out which calib IDs need processing
        calib_ids_to_process = []
        for calib_id in calib_ids:
            if self._should_process(calib_id, raw_data_ids):
                calib_ids_to_process.append(calib_id)

        self.logger.info(f"{len(calib_ids_to_process)} calib IDs require processing for"
                         f" calib_date={calib_date}.")

        if len(calib_ids_to_process) == 0:
            return

        calibs_existing = [c for c in calib_ids if os.path.isfile(c["filename"])]
        calibs_ingest = [c for c in calibs_existing if c not in calib_ids_to_process]

        # Figure out if we can skip any of the calibs
        skip_bias = not any([_["datasetType"] == "bias" for _ in calib_ids])
        skip_dark = skip_bias and not any([_["datasetType"] == "dark" for _ in calib_ids])

        # Process data in a temporary butler repo
        with TemporaryButlerRepository() as br:

            # Ingest raw exposures
            br.ingest_raw_data([_["filename"] for _ in raw_data_ids])

            # Ingest existing master calibs
            for calib_type in self._calib_types:
                filenames = [c["filename"] for c in calibs_ingest if c["calibType"] == calib_type]
                if filenames:
                    br.ingest_master_calibs(calib_type, filenames=filenames,
                                            validity=self._validity)

            # Make and archive the master calibs
            try:
                self.logger.info(f"Making master calibs for calib_date={calib_date}.")
                br.make_master_calibs(skip_bias=skip_bias, skip_dark=skip_dark,
                                      calib_date=calib_date)
                br.archive_master_calibs()

            except Exception as err:
                self.logger.warning(f"Unable to create master calib for calib_date={calib_date}:"
                                    f" {err!r}")

    # Private methods

    def _run(self):
        """ Call self.process_date for each unique calib date in the raw exposure table. """

        calib_dates = self._get_unique_dates()
        self.logger.info(f"Found {len(calib_dates)} unique calib dates.")

        for calib_date in calib_dates:
            self.logger.info(f"Processing calibs for calib_date={calib_date}.")

            if self._stop_threads:
                return

            self.process_date(calib_date)

    def _should_process(self, calib_id, raw_data_ids):
        """ Check if the given calib_id should be processed based on existing raw data.
        Args:
            calib_id (CalibId): The calib ID.
            raw_data_ids (list of DataId): The raw exposure dataIDs.
        Returns:
            bool: True if the calib ID requires processing, else False.
        """
        query_result = self._calib_table.find(document_filter=calib_id)

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
        """
        """
        parsed_date = parse_date(calib_date)
        date_start = parsed_date - self._validity
        date_end = parsed_date + self._validity

        result = self._exposure_table.find(date_start=date_start, date_end=date_end, screen=True)
        self.logger.info(f"Found {len(result)} raw calibs for calib_date={calib_date}.")

        return result

    def _get_unique_calib_ids(self, calib_date, documents):
        """
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

            calib_id = CalibId(calib_dict)
            if calib_id not in unique_calib_ids:
                unique_calib_ids.append(calib_id)

        return unique_calib_ids

    def _get_unique_dates(self):
        """ Get all calib dates specified by files in the raw data table.
        Returns:
            list of datetime: The list of dates.
        """
        dates = set([date_to_ymd(d) for d in self._exposure_table.find(key="date")])
        return list(dates)
