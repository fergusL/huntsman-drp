import pytest

from huntsman.drp.core import get_config
from huntsman.drp.fitsutil import FitsHeaderTranslator
from huntsman.drp.collection import RawExposureCollection
from huntsman.drp.refcat import TapReferenceCatalogue
from huntsman.drp.lsst.butler import ButlerRepository, TemporaryButlerRepository
from huntsman.drp.utils import testing
from huntsman.drp.calib import MasterCalibMaker

# ===========================================================================
# Config


@pytest.fixture(scope="function")
def config():
    config = get_config(ignore_local=True, testing=True)

    # Hack around so files pass screening and quality cuts
    # TODO: Move to testing config
    for k in ("bias", "dark", "flat", "science"):
        if k in config["quality"]["raw"]:
            del config["quality"]["raw"][k]

    return config

# ===========================================================================
# Reference catalogue


@pytest.fixture(scope="function")
def reference_catalogue(config):
    return TapReferenceCatalogue(config=config)

# ===========================================================================
# Butler repositories


@pytest.fixture(scope="function")
def temp_butler_repo(config):
    return TemporaryButlerRepository(config=config)


@pytest.fixture(scope="function")
def fixed_butler_repo(config, tmp_path_factory):
    dir = tmp_path_factory.mktemp("fixed_butler_repo")
    return ButlerRepository(directory=str(dir), config=config)


@pytest.fixture(scope="function")
def butler_repos(fixed_butler_repo, temp_butler_repo):
    return fixed_butler_repo, temp_butler_repo


# ===========================================================================
# Testing data


@pytest.fixture(scope="function")
def fits_header_translator(config):
    return FitsHeaderTranslator(config=config)


@pytest.fixture(scope="function")
def exposure_table(tmp_path_factory, config, fits_header_translator):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("test_exposure_sequence")
    expseq = testing.FakeExposureSequence(config=config)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    exposure_table = RawExposureCollection(config=config, table_name="fake_data")
    for filename, header in expseq.header_dict.items():

        # Parse the header
        parsed_header = fits_header_translator.parse_header(header)
        parsed_header["filename"] = filename

        # Insert the parsed header into the DB table
        exposure_table.insert_one(parsed_header)

    # Make sure table has the correct number of rows
    assert exposure_table.count_documents() == expseq.file_count
    yield exposure_table

    # Remove the metadata from the DB ready for other tests
    all_metadata = exposure_table.find()
    exposure_table.delete_many(all_metadata)


@pytest.fixture(scope="function")
def exposure_table_real_data(config, fits_header_translator):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Populate the database
    exposure_table = testing.create_test_exposure_table(config, fits_header_translator, screen=True)

    yield exposure_table

    # Remove the metadata from the DB ready for other tests
    all_metadata = exposure_table.find()
    exposure_table.delete_many(all_metadata)


@pytest.fixture(scope="function")
def master_calib_table_real_data(exposure_table_real_data, config):
    """ Make a master calib table by reducing real calib data.
    TODO: Store created files so they can be copied in for quicker tests.
    """
    calib_maker = MasterCalibMaker(exposure_table=exposure_table_real_data, config=config)
    calib_maker.logger.info("Creating master calibs for tests.")

    # Make master calibs
    dates = calib_maker._get_unique_dates()
    for date in dates[:1]:  # Limit to one date for now
        calib_maker.process_date(date)

    calib_maker.logger.info("Finished creating master calibs for tests.")

    calib_table = calib_maker._calib_table
    yield calib_table

    # Remove the metadata from the DB ready for other tests
    all_metadata = calib_table.find()
    calib_table.delete_many(all_metadata)


@pytest.fixture(scope="function")
def tempdir_and_exposure_table_with_uningested_files(
        tmp_path_factory, config, fits_header_translator):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("dir_with_uningested_files")
    expseq = testing.FakeExposureSequence(config=config)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    exposure_table = RawExposureCollection(config=config, table_name="fake_data")
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
        exposure_table.insert_one(parsed_header)

    # Make sure table has the correct number of rows
    assert exposure_table.count_documents() == n_stop
    yield (tempdir, exposure_table)

    # Remove the metadata from the DB ready for other tests
    all_metadata = exposure_table.find()
    exposure_table.delete_many(all_metadata)
