import pytest
import time
from copy import deepcopy

from huntsman.drp.core import get_config
from huntsman.drp.fitsutil import FitsHeaderTranslator
from huntsman.drp.collection import RawExposureCollection
from huntsman.drp import refcat as rc
from huntsman.drp.utils import testing
from huntsman.drp.services.calib import MasterCalibMaker

# ===========================================================================
# Config


@pytest.fixture(scope="session")
def session_config():
    """ Session scope config dict to be used for creating shared fixtures """

    config = get_config(ignore_local=True, testing=True)

    # Hack around so files pass screening and quality cuts
    # TODO: Move to testing config
    for k in ("bias", "dark", "flat", "science"):
        if k in config["quality"]["raw"]:
            del config["quality"]["raw"][k]

    return config


@pytest.fixture(scope="function")
def config(session_config):
    """ Function scope version of config_module that should be used in tests """
    return deepcopy(session_config)

# ===========================================================================
# Reference catalogue


@pytest.fixture(scope="session")
def refcat_filename(session_config):
    return testing.get_refcat_filename(config=session_config)


@pytest.fixture(scope="session")
def testing_refcat_server(session_config, refcat_filename):
    """ A testing refcat server that loads the refcat from file rather than downloading it.
    """
    refcat_kwargs = dict(refcat_filename=refcat_filename)

    # Yield the refcat server process
    refcat_service = rc.create_refcat_service(refcat_type=rc.TestingTapReferenceCatalogue,
                                              refcat_kwargs=refcat_kwargs,
                                              config=session_config)
    refcat_service.start()
    time.sleep(5)  # Allow some startup time
    yield refcat_service

    # Shutdown the refcat server after we are done
    refcat_service.stop()


# ===========================================================================
# Testing data


@pytest.fixture(scope="function")
def exposure_collection(tmp_path_factory, config):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    fits_header_translator = FitsHeaderTranslator(config=config)

    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("test_exposure_sequence")
    expseq = testing.FakeExposureSequence(config=config)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    exposure_collection = RawExposureCollection(config=config, collection_name="fake_data")
    exposure_collection.delete_all(really=True)

    for filename, header in expseq.header_dict.items():

        # Parse the header
        parsed_header = fits_header_translator.parse_header(header)
        parsed_header["filename"] = filename

        # Insert the parsed header into the DB table
        exposure_collection.insert_one(parsed_header)

    # Make sure table has the correct number of rows
    assert exposure_collection.count_documents() == expseq.file_count
    yield exposure_collection

    # Remove the metadata from the DB ready for other tests
    exposure_collection.delete_all(really=True)


@pytest.fixture(scope="session")
def exposure_collection_real_data(session_config):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Populate the database
    exposure_collection = testing.create_test_exposure_collection(session_config, clear=True)

    yield exposure_collection

    # Remove the metadata from the DB ready for other tests
    exposure_collection.logger.info("Deleting all documents after test.")
    exposure_collection.delete_all(really=True)
    assert not exposure_collection.find()


@pytest.fixture(scope="session")
def master_calib_collection_real_data(exposure_collection_real_data, session_config):
    """ Make a master calib table by reducing real calib data.
    TODO: Store created files so they can be copied in for quicker tests.
    """
    calib_maker = MasterCalibMaker(exposure_collection=exposure_collection_real_data,
                                   config=session_config)
    calib_maker.logger.info("Creating master calibs for tests.")

    # Make master calibs
    dates = calib_maker._get_unique_dates()
    for date in dates[:1]:  # Limit to one date for now
        calib_maker.process_date(date)

    calib_maker.logger.info("Finished creating master calibs for tests.")

    calib_collection = calib_maker._calib_collection
    yield calib_collection

    # Remove the metadata from the DB ready for other tests
    all_metadata = calib_collection.find()
    calib_collection.delete_many(all_metadata)


@pytest.fixture(scope="function")
def tempdir_and_exposure_collection_with_uningested_files(tmp_path_factory, config,
                                                          exposure_collection):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    fits_header_translator = FitsHeaderTranslator(config=config)

    # Clear the exposure collection of any existing documents
    exposure_collection.delete_all(really=True)

    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("dir_with_uningested_files")
    expseq = testing.FakeExposureSequence(config=config)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    n_stop = len(expseq.header_dict) * 0.7 // 1  # ingest ~70% of the files
    n = 0
    for filename, header in expseq.header_dict.items():
        if n >= n_stop:
            break
        n += 1

        # Parse the header
        parsed_header = fits_header_translator.parse_header(header)
        parsed_header["filename"] = filename

        # Insert the parsed header into the DB table
        exposure_collection.insert_one(parsed_header)

    # Make sure table has the correct number of rows
    assert exposure_collection.count_documents() == n_stop
    yield (tempdir, exposure_collection)

    # Remove the metadata from the DB ready for other tests
    exposure_collection.delete_all(really=True)
