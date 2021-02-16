import pytest
from astropy.io import fits

from huntsman.drp.core import get_config
from huntsman.drp.tests.data import FakeExposureSequence
from huntsman.drp.fitsutil import FitsHeaderTranslator
from huntsman.drp.datatable import ExposureTable
from huntsman.drp.refcat import TapReferenceCatalogue
from huntsman.drp.butler import ButlerRepository, TemporaryButlerRepository
from huntsman.drp.utils.testing import get_testdata_fits_filenames

# ===========================================================================
# Config


@pytest.fixture(scope="function")
def config():
    return get_config(ignore_local=True, testing=True)

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
    expseq = FakeExposureSequence(config=config)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    exposure_table = ExposureTable(config=config, table_name="fake_data")
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
    exposure_table = ExposureTable(config=config, table_name="real_data")

    for filename in get_testdata_fits_filenames(config=config):

        # Parse the header
        header = fits.getheader(filename)
        parsed_header = fits_header_translator.parse_header(header)
        parsed_header["filename"] = filename

        # Insert the parsed header into the DB table
        exposure_table.insert_one(parsed_header)

    yield exposure_table

    # Remove the metadata from the DB ready for other tests
    all_metadata = exposure_table.find()
    exposure_table.delete_many(all_metadata)


@pytest.fixture(scope="function")
def tempdir_and_exposure_table_with_uningested_files(
        tmp_path_factory, config, fits_header_translator):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("dir_with_uningested_files")
    expseq = FakeExposureSequence(config=config)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    exposure_table = ExposureTable(config=config, table_name="fake_data")
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
