import time
from threading import Thread

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.library import load_module
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection
from huntsman.drp.lsst.butler import TemporaryButlerRepository
from huntsman.drp.quality.metrics.calexp import METRICS


def get_quality_metrics(calexp):
    """ Evaluate metrics for a single calexp. This could probably be improved in future.
    TODO: Implement version control here.
    """
    result = {}
    for metric in METRICS:
        func = load_module(f"huntsman.drp.quality.metrics.calexp.{metric}")
        result[metric] = func(calexp)
    return result


class CalexpQualityMonitor(HuntsmanBase):
    """ Class to continually evauate and archive calexp quality metrics for raw exposures that
    have not already been processed. Intended to run as a docker service.
    """

    def __init__(self, sleep=300, exposure_table=None, calib_table=None, refcat_filename=None,
                 *args, **kwargs):
        """
        Args:
            sleep (float): Time to sleep if there are no new files that require processing. Default
                300s.
            exposure_table (Collection, optional): The exposure table. If not given, will create
                a new RawExposureCollection instance.
            calib_table (Collection, optional): The master calib table. If not given, will create
                a new MasterCalibCollection instance.
            refcat_filename (str, optional): The reference catalogue filename. If not provided,
                will create a new refcat.
        """
        super().__init__(*args, **kwargs)
        self._sleep = sleep
        self._refcat_filename = refcat_filename

        self._stop = False
        self._data_ids = set()
        self._n_processed = 0
        self._n_failed = 0

        if exposure_table is None:
            exposure_table = RawExposureCollection(config=self.config, logger=self.logger)
        self._exposure_table = exposure_table

        if calib_table is None:
            calib_table = MasterCalibCollection(config=self.config, logger=self.logger)
        self._calib_table = calib_table

        self._calexp_thread = Thread(target=self._async_process_files)

    @property
    def is_running(self):
        """ Check if the monitor is running.
        Returns:
            bool: True if running, else False.
        """
        return self.status["running"]

    @property
    def n_queued(self):
        return len(self._data_ids)

    @property
    def status(self):
        """ Return the status of the quality monitor.
        Returns:
            dict: The status dict.
        """
        status = {"processed": self._n_processed,
                  "failed": self._n_failed,
                  "queued": self.n_queued,
                  "running": self._calexp_thread.is_alive()}
        return status

    def start(self):
        """ Start the calexp montioring thread. """
        self.logger.info("Starting calexp monitor thread.")
        self._stop = False
        self._calexp_thread.start()

    def stop(self):
        """ Stop the calexp monitoring thread.
        Note that this will block until any ongoing processing has finished.
        """
        self.logger.info("Stopping calexp monitor thread.")
        self._stop = True
        self._calexp_thread.join()

    def _refresh_data_ids(self):
        """ Update the set of data IDs that require processing. """
        data_ids = self._exposure_table.find({"dataType": "science"}, screen=True,
                                             quality_filter=True)
        self._data_ids.update([d for d in data_ids if self._requires_processing(d)])

    def _async_process_files(self):
        """ Continually check for and process files that require processing. """
        self.logger.debug("Starting processing thread.")

        while True:
            if self._stop:
                self.logger.debug("Stopping calexp thread.")
                break

            self._refresh_data_ids()
            self.logger.info(f"Status: {self.status}")

            # Sleep if no new files
            if self.n_queued == 0:
                self.logger.info(f"No files to process. Sleeping for {self._sleep}s.")
                time.sleep(self._sleep)
                continue

            data_id = self._data_ids.pop()
            self.logger.info(f"Processing data ID: {data_id}")
            self._process_file(data_id)

            time.sleep(1)

    def _process_file(self, data_id):
        """ Create a calibrated exposure (calexp) for the given data ID and store the metadata.
        Args:
            data_id (DataId): The data ID.
        """
        # Get matching master calibs. If no matching calib, log warning and return.
        calib_date = data_id["dateObs"]
        try:
            calibs = self._calib_table.get_matching_calibs(data_id, calib_date=calib_date)
        except FileNotFoundError as err:
            self.logger.warning(f"{err!r}")
            self._n_failed += 1
            return

        with TemporaryButlerRepository() as br:

            # Ingest raw science exposure into the bulter repository
            br.ingest_raw_data([data_id["filename"]])

            # Ingest the corresponding master calibs
            for calib_type, calib_id in calibs.items():
                calib_filename = calib_id["filename"]
                br.ingest_master_calibs(datasetType=calib_type, filenames=[calib_filename])

            # Make and ingest the reference catalogue
            if self._refcat_filename is None:
                br.make_reference_catalogue()
            else:
                br.ingest_reference_catalogue([self._refcat_filename])

            # Make the calexps, also getting the dataIds to match with their raw frames
            try:
                br.make_calexps()
            except Exception as err:
                self.logger.warning(f"Unable to create calexp for {data_id}: {err!r}")
                self._n_failed += 1
                return

            required_keys = br.get_keys("raw")
            calexps, dataIds = br.get_calexps(extra_keys=required_keys)

            # Evaluate metrics and insert into the database
            for calexp, calexp_id in zip(calexps, dataIds):

                metrics = get_quality_metrics(calexp)

                # Make the document and update the DB
                document = {k: calexp_id[k] for k in required_keys}
                to_update = {"quality": {"calexp": metrics}}
                self._exposure_table.update_one(document, to_update=to_update)

        self._n_processed += 1

    def _requires_processing(self, file_info):
        """ Check if a file requires processing.
        Args:
            file_info (dict): The file document from the exposure data table.
        Returns:
            bool: True if processing required, else False.
        """
        try:
            return "calexp" not in file_info["quality"]
        except KeyError:
            return True
