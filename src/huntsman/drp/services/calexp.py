import tempfile
from functools import partial
from multiprocessing.pool import ThreadPool

from huntsman.drp.services.base import ProcessQueue
from huntsman.drp.utils.library import load_module
from huntsman.drp.lsst.butler import TemporaryButlerRepository
from huntsman.drp.metrics.calexp import METRICS
from huntsman.drp.refcat import RefcatClient


def _get_quality_metrics(calexp):
    """ Evaluate metrics for a single calexp. This could probably be improved in future.
    TODO: Implement version control here.
    """
    result = {}
    for metric in METRICS:
        func = load_module(f"huntsman.drp.metrics.calexp.{metric}")
        result[metric] = func(calexp)
    return result


def _process_document(document, exposure_collection, calib_collection, timeout, **kwargs):
    """ Create a calibrated exposure (calexp) for the given data ID and store the metadata.
    Args:
        document (RawExposureDocument): The document to process.
    """
    config = exposure_collection.config
    logger = calib_collection.logger

    logger.info(f"Processing document: {document}")

    # Get matching calibs for this document
    # If there is no matching set, this will raise an error
    calib_docs = calib_collection.get_matching_calibs(document)

    # Use a directory prefix for the temporary directory
    # This is necessary as the tempfile module is apparently creating duplicates(!)
    directory_prefix = document["expId"]

    with TemporaryButlerRepository(logger=logger, config=config,
                                   calib_collection=calib_collection,
                                   directory_prefix=directory_prefix) as br:

        logger.debug(f"Butler directory for {document}: {br.butler_dir}")

        # Ingest raw science exposure into the bulter repository
        logger.debug(f"Ingesting raw data for {document}")
        br.ingest_raw_data([document["filename"]])

        # Check the files were ingested properly
        # This shouldn't be neccessary but helps for debugging
        ingested_docs = br.get_dataIds("raw")
        if len(ingested_docs) != 1:
            raise RuntimeError(f"Unexpected number of ingested raw files: {len(ingested_docs)}")

        # Ingest the corresponding master calibs
        logger.debug(f"Ingesting master calibs for {document}")

        for calib_type, calib_doc in calib_docs.items():
            calib_filename = calib_doc["filename"]

            # Use a high validity as the calib matching is already taken care of
            br.ingest_master_calibs(datasetType=calib_type, filenames=[calib_filename],
                                    validity=1000)

        # Make and ingest the reference catalogue
        logger.debug(f"Making refcat for {document}")
        refcat_client = RefcatClient(config=config, logger=logger)

        with tempfile.NamedTemporaryFile(prefix=directory_prefix) as tf:
            try:
                # Download the refcat to the tempfile
                refcat_client.make_from_documents([document], filename=tf.name)
            except Exception as err:
                logger.error(f"Exception while making refcat for {document}: {err!r}")
                raise err
            finally:
                # Cleanup the refcat client
                # This *shouldn't* be necessary but seems like it might be...
                # TODO: Parse refcat client as function arg?
                refcat_client._proxy._pyroRelease()

            br.ingest_reference_catalogue([tf.name])

        # Make the calexp
        logger.debug(f"Making calexp for {document}")
        br.make_calexps(timeout=timeout)
        required_keys = br.get_keys("raw")

        # Retrieve the calexp objects and their data IDs
        calexps, dataIds = br.get_calexps(extra_keys=required_keys)
        if len(calexps) != 1:
            raise RuntimeError(f"Unexpected number of calexps: {len(ingested_docs)}")

        calexp = calexps[0]
        calexpId = dataIds[0]

        # Evaluate metrics and insert into the database
        logger.debug(f"Calculating metrics for {document}")

        metrics = _get_quality_metrics(calexp)

        # Make the document and update the DB
        document_filter = {k: calexpId[k] for k in required_keys}
        to_update = {"metrics": {"calexp": metrics}}
        exposure_collection.update_one(document_filter, to_update=to_update)


class CalexpQualityMonitor(ProcessQueue):
    """ Class to continually evauate and archive calexp quality metrics for raw exposures that
    have not already been processed. Intended to run as a docker service.
    """
    _pool_class = ThreadPool  # Use ThreadPool as LSST code makes its own subprocesses

    def __init__(self, nproc=None, timeout=None, *args, **kwargs):
        """
        Args:
            nproc (int): The number of processes to use. If None (default), will check the config
                item `calexp-monitor.nproc` with a default value of 1.
        """
        super().__init__(*args, **kwargs)

        calexp_config = self.config.get("calexp-monitor", {})

        # Set the number of processes
        if nproc is None:
            nproc = calexp_config.get("nproc", 1)
        self._nproc = int(nproc)
        self.logger.debug(f"Calexp monitor using {nproc} processes.")

        # Specify timeout for calexp processing
        self._timeout = timeout if timeout is not None else calexp_config.get("timeout", None)

    def _async_process_objects(self, *args, **kwargs):
        """ Continually process objects in the queue. """

        func = partial(_process_document, timeout=self._timeout)

        return super()._async_process_objects(process_func=func)

    def _get_objs(self):
        """ Update the set of data IDs that require processing. """
        docs = self._exposure_collection.find({"dataType": "science"}, screen=True,
                                              quality_filter=True)
        return [d for d in docs if self._requires_processing(d)]

    def _requires_processing(self, document):
        """ Check if a document requires processing.
        Args:
            document (Document): The document to check.
        Returns:
            bool: True if processing required, else False.
        """
        try:
            return "calexp" not in document["metrics"]
        except KeyError:
            return True
