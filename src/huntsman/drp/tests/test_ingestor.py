import os
import time
import pytest

from huntsman.drp.services.ingestor import FileIngestor
from huntsman.drp.utils.ingest import METRIC_SUCCESS_FLAG, screen_success


@pytest.fixture(scope="function")
def ingestor(tempdir_and_exposure_collection_with_uningested_files, config):
    """
    """
    tempdir, exposure_collection = tempdir_and_exposure_collection_with_uningested_files

    # Make sure the ingestor uses the correct collection
    raw_name = exposure_collection.collection_name
    config["mongodb"]["collections"]["RawExposureCollection"]["name"] = raw_name

    tempdir, exposure_collection = tempdir_and_exposure_collection_with_uningested_files

    ingestor = FileIngestor(queue_interval=10, status_interval=5, directory=tempdir, config=config)

    # Skip astrometry tasks as tests running in drp-lsst container
    ingestor._raw_metrics = [_ for _ in ingestor._raw_metrics if _ != "get_wcs"]

    yield ingestor

    ingestor.stop()


def test_file_ingestor(ingestor, tempdir_and_exposure_collection_with_uningested_files, config):
    """This test runs on a directory where ~70% of the images have already been
    ingested into the datatable. The files already in the datatable should be
    identified as requiring screening. The uningested files should be ingested
    and then should be picked up for screening as well"""

    tempdir, exposure_collection = tempdir_and_exposure_collection_with_uningested_files

    n_to_process = len(os.listdir(tempdir))
    assert n_to_process > 0

    ingestor.start()
    i = 0
    timeout = 20

    while (i < timeout):
        if ingestor.is_running and ingestor.status["processed"] == n_to_process:
            break
        i += 1
        time.sleep(1)

    if i == timeout:
        raise RuntimeError(f"Timeout while waiting for processing of {n_to_process} images.")

    if not ingestor.is_running:
        raise RuntimeError("Ingestor has stopped running.")

    ingestor.stop(blocking=True)
    assert not ingestor.is_running

    assert ingestor._n_failed == 0

    for md in exposure_collection.find():
        ingestor.logger.info(f"{md}")
        assert md.get(f"metrics.{METRIC_SUCCESS_FLAG}", False)
        assert screen_success(md)

    for metric_value in md["metrics"].values():
        assert metric_value is not None

    ingestor.stop(blocking=True)
    assert not ingestor.is_running
