import pytest

from huntsman.drp.core import get_config
from huntsman.drp.tests.data import FakeExposureSequence
from huntsman.drp.fitsutil import FitsHeaderTranslator
from huntsman.drp.datatable import RawDataTable, RawQualityTable
from huntsman.drp.refcat import TapReferenceCatalogue
from huntsman.drp.butler import ButlerRepository, TemporaryButlerRepository

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
def raw_data_table(tmp_path_factory, config, fits_header_translator):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("test_exposure_sequence")
    expseq = FakeExposureSequence(config=config)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    raw_data_table = RawDataTable(config=config)
    raw_data_table.unlock()
    for filename, header in expseq.header_dict.items():
        # Parse the header
        parsed_header = fits_header_translator.parse_header(header)
        parsed_header["filename"] = filename
        # Insert the parsed header into the DB table
        raw_data_table.insert(parsed_header)

    # Make sure table has the correct number of rows
    assert len(raw_data_table.query()) == expseq.file_count
    yield raw_data_table

    # Remove the metadata from the DB ready for other tests
    all_metadata = raw_data_table.query()
    raw_data_table.delete(all_metadata)


@pytest.fixture(scope="function")
def raw_quality_table(config):
    table = RawQualityTable(config=config)
    yield table
    all_metadata = table.query()
    table.delete(all_metadata)
