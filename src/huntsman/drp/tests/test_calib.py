import os
import time
import pytest

from panoptes.utils.time import CountdownTimer
from panoptes.utils import error

from huntsman.drp.utils.testing import FakeExposureSequence
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection
from huntsman.drp.services.calib import MasterCalibMaker
from huntsman.drp.utils.ingest import METRIC_SUCCESS_FLAG
from huntsman.drp.fitsutil import FitsHeaderTranslator


@pytest.fixture(scope="function")
def config_lite(config):
    """ A config containing a smaller exposure sequence. """
    config["exposure_sequence"]["n_days"] = 1
    config["exposure_sequence"]["n_cameras"] = 1
    config["exposure_sequence"]["n_dark"] = 1
    config["exposure_sequence"]["n_bias"] = 1
    config["exposure_sequence"]["filters"] = ["g_band"]
    return config


@pytest.fixture(scope="function")
def empty_calib_collection(config):
    """ An empty master calib collection. """
    col = MasterCalibCollection(config=config, collection_name="master_calib_test")
    yield col

    col.delete_all(really=True)


@pytest.fixture(scope="function")
def exposure_collection_lite(tmp_path_factory, config_lite):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    fits_header_translator = FitsHeaderTranslator(config=config_lite)

    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("test_exposure_sequence")
    expseq = FakeExposureSequence(config=config_lite)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    exposure_collection = RawExposureCollection(config=config_lite,
                                                collection_name="fake_data_lite")

    for filename, header in expseq.header_dict.items():

        # Parse the header
        parsed_header = fits_header_translator.parse_header(header)
        parsed_header["filename"] = filename
        parsed_header[METRIC_SUCCESS_FLAG] = True

        # Insert the parsed header into the DB table
        exposure_collection.insert_one(parsed_header)

    # Make sure table has the correct number of rows
    assert exposure_collection.count_documents() == expseq.file_count
    yield exposure_collection

    # Remove the metadata from the DB ready for other tests
    all_metadata = exposure_collection.find()
    exposure_collection.delete_many(all_metadata)


@pytest.fixture(scope="function")
def calib_maker(config, exposure_collection_lite, empty_calib_collection):
    calib_maker = MasterCalibMaker(config=config, exposure_collection=exposure_collection_lite,
                                   calib_collection=empty_calib_collection)
    yield calib_maker
    calib_maker.stop()


def test_master_calib_maker(calib_maker, config):

    n_calib_dates = config["exposure_sequence"]["n_days"]
    n_cameras = config["exposure_sequence"]["n_cameras"]
    n_filters = len(config["exposure_sequence"]["filters"])

    n_flats = n_calib_dates * n_filters * n_cameras
    n_bias = n_calib_dates * n_cameras
    n_dark = n_calib_dates * n_cameras

    calib_collection = calib_maker._calib_collection
    assert not calib_collection.find()  # Check calib table is empty

    assert not calib_maker.is_running
    calib_maker.start()
    assert calib_maker.is_running

    timer = CountdownTimer(duration=300)
    while not timer.expired():
        calib_maker.logger.debug("Waiting for calibs...")

        dataset_types = calib_collection.find(key="datasetType")

        # Check if we are finished
        if len([d for d in dataset_types if d == "flat"]) == n_flats:
            if len([d for d in dataset_types if d == "bias"]) == n_bias:
                if len([d for d in dataset_types if d == "dark"]) == n_dark:
                    break

        for filename in calib_collection.find(key="filename"):
            assert os.path.isfile(filename)

        if not calib_maker.is_running:
            raise RuntimeError("Calib maker has stopped running. Check the logs for details.")

        time.sleep(10)

    if timer.expired():
        raise error.Timeout("Timeout while waiting for calibs.")

    calib_maker.stop()
    assert not calib_maker.is_running
