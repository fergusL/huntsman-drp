from functools import partial
from copy import deepcopy

from huntsman.drp.services.base import ProcessQueue
from huntsman.drp.fitsutil import FitsHeaderTranslator, read_fits_header, read_fits_data
from huntsman.drp.utils import load_module
from huntsman.drp.metrics.raw import RAW_METRICS
from huntsman.drp.utils.ingest import METRIC_SUCCESS_FLAG, list_fits_files_recursive


def _process_file(filename, metric_names, exposure_collection, **kwargs):
    """ Process a single file.
    This function has to be defined outside of the FileIngestor class since we are using
    multiprocessing and class instance methods cannot be pickled.
    Args:
        filename (str): The name of the file to process.
        metric_names (list of str): The list of the metrics to process.
        exposure_collection (RawExposureCollection): The raw exposure collection.
    Returns:
        bool: True if file was successfully processed, else False.
    """
    config = exposure_collection.config
    logger = exposure_collection.logger

    logger.debug(f"Processing file: {filename}.")
    fits_header_translator = FitsHeaderTranslator(config=config, logger=logger)

    # Read the header
    parsed_header = fits_header_translator.read_and_parse(filename)

    # Get the metrics
    metrics, success = _get_raw_metrics(filename, metric_names=metric_names, logger=logger)
    metrics[METRIC_SUCCESS_FLAG] = success
    to_update = {"metrics": metrics}

    # Update the document (upserting if necessary)
    to_update.update(parsed_header)
    to_update["filename"] = filename
    exposure_collection.update_one(parsed_header, to_update=to_update, upsert=True)

    # Raise an exception if not success
    if not success:
        raise RuntimeError(f"Metric evaluation unsuccessful for {filename}.")


def _get_raw_metrics(filename, metric_names, logger):
    """ Evaluate metrics for a raw/unprocessed file.
    Args:
        filename (str): The filename of the FITS image to be processed.
        metric_names (list of str): The list of the metrics to process.
    Returns:
        dict: Dictionary containing the metric names / values.
    """
    result = {}
    success = True

    # Read the FITS file
    try:
        header = read_fits_header(filename)
        data = read_fits_data(filename)  # Returns float array
    except Exception as err:
        logger.error(f"Unable to read {filename}: {err!r}")
        success = False
    else:
        for metric in metric_names:
            func = load_module(f"huntsman.drp.metrics.raw.{metric}")
            try:
                result.update(func(filename, data=data, header=header))
            except Exception as err:
                logger.error(f"Exception while calculating {metric} for {filename}: {err!r}")
                success = False

    return result, success


class FileIngestor(ProcessQueue):
    """ Class to watch for new file entries in database and process their metadata. """

    # Work around so that tests can run without running the has_wcs metric
    _raw_metrics = deepcopy(RAW_METRICS)

    def __init__(self, directory=None, nproc=None, *args, **kwargs):
        """
        Args:
            directory (str): The top level directory to watch for new files, so they can
                be added to the relevant datatable.
            nproc (int): The number of processes to use. If None (default), will check the config
                item `screener.nproc` with a default value of 1.
            *args, **kwargs: Parsed to ProcessQueue initialiser.
        """
        super().__init__(*args, **kwargs)

        ingestor_config = self.config.get("ingestor", {})

        # Set the number of processes
        if nproc is None:
            nproc = ingestor_config.get("nproc", 1)
        self._nproc = int(nproc)

        # Set the monitored directory
        if directory is None:
            directory = ingestor_config["directory"]
        self._directory = directory
        self.logger.debug(f"Ingesting files in directory: {self._directory}")

    def _async_process_objects(self, *args, **kwargs):
        """ Continually process objects in the queue. """

        func = partial(_process_file, metric_names=self._raw_metrics)

        return super()._async_process_objects(process_func=func)

    def _get_objs(self):
        """ Get list of files to process. """
        # Get set of all files in watched directory
        files_in_directory = set(list_fits_files_recursive(self._directory))
        self.logger.debug(f"Found {len(files_in_directory)} FITS files in {self._directory}.")

        # Get set of all files that are ingested and pass screening
        files_ingested = set(self.exposure_collection.find(screen=True, key="filename"))

        # Identify files that require processing
        files_to_process = files_in_directory - files_ingested
        self.logger.debug(f"Found {len(files_to_process)} files requiring processing.")

        return files_to_process
