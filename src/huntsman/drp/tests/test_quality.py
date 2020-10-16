import pytest

from huntsman.drp import quality
from astropy.io import fits


@pytest.fixture(scope="module")
def data_list(raw_data_table):
    """
    Load a small amount of data to run the tests on.
    """
    filenames = raw_data_table.query_column("filename", dataType="science")[:2]
    return [fits.getdata(f) for f in filenames]


def test_get_metadata(data_list, config):
    """
    Placeholder for a more detailed test.
    """
    mds = []
    for data in data_list:
        mds.append(quality.get_metadata(data, config=config))
