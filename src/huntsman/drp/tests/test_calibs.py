import time
import pytest

from panoptes.utils.time import CountdownTimer
from panoptes.utils import error

from huntsman.drp.utils.testing import FakeExposureSequence
from huntsman.drp.datatable import ExposureTable
from huntsman.drp.calib import MasterCalibMaker
from huntsman.drp.utils.screening import SCREEN_SUCCESS_FLAG


@pytest.fixture(scope="function")
def config_lite(config):
    """ A config containing a smaller exposure sequence. """
    config = config.copy()
    config["exposure_sequence"]["n_days"] = 1
    config["exposure_sequence"]["n_cameras"] = 1
    config["exposure_sequence"]["n_dark"] = 1
    return config


@pytest.fixture(scope="function")
def exposure_table_lite(tmp_path_factory, config_lite, fits_header_translator):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("test_exposure_sequence")
    expseq = FakeExposureSequence(config=config_lite)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    exposure_table = ExposureTable(config=config_lite, table_name="fake_data_lite")
    for filename, header in expseq.header_dict.items():

        # Parse the header
        parsed_header = fits_header_translator.parse_header(header)
        parsed_header["filename"] = filename
        parsed_header[SCREEN_SUCCESS_FLAG] = True

        # Insert the parsed header into the DB table
        exposure_table.insert_one(parsed_header)

    # Make sure table has the correct number of rows
    assert exposure_table.count_documents() == expseq.file_count
    yield exposure_table

    # Remove the metadata from the DB ready for other tests
    all_metadata = exposure_table.find()
    exposure_table.delete_many(all_metadata)


@pytest.fixture(scope="function")
def calib_maker(config, exposure_table_lite):
    calib_maker = MasterCalibMaker(config=config, exposure_table=exposure_table_lite)
    yield calib_maker
    calib_maker.stop()


def test_master_calib_maker(calib_maker, config):

    n_calib_dates = config["exposure_sequence"]["n_days"]
    n_cameras = config["exposure_sequence"]["n_cameras"]
    n_filters = len(config["exposure_sequence"]["filters"])

    n_flats = n_calib_dates * n_filters * n_cameras
    n_bias = n_calib_dates * n_cameras
    n_dark = n_calib_dates * n_cameras

    calib_table = calib_maker._calib_table
    assert not calib_table.find()  # Check calib table is empty

    assert not calib_maker.is_running
    calib_maker.start()
    assert calib_maker.is_running

    timer = CountdownTimer(duration=300)
    while not timer.expired():
        calib_maker.logger.debug("Waiting for calibs...")

        dataset_types = calib_table.find(key="datasetType")

        # Check if we are finished
        if len([d for d in dataset_types if d == "flat"]) == n_flats:
            if len([d for d in dataset_types if d == "bias"]) == n_bias:
                if len([d for d in dataset_types if d == "dark"]) == n_dark:
                    break

        if not calib_maker.is_running:
            raise RuntimeError("Calib maker has stopped running. Check the logs for details.")

        time.sleep(10)

    if timer.expired():
        raise error.Timeout("Timeout while waiting for calibs.")

    calib_maker.stop()
    assert not calib_maker.is_running
