import pytest

from huntsman.drp import quality


@pytest.fixture(scope="function")
def metadata_dataframe(exposure_table):
    """ Load a small amount of data to run the tests on. """
    criteria = dict(dataType="science")
    return exposure_table.get_metrics(criteria=criteria)[:2]


def test_metadata_from_fits(metadata_dataframe, config):
    """ Placeholder for a more detailed test. """
    mds = []
    for i in range(metadata_dataframe.shape[0]):
        mds.append(quality.metadata_from_fits(metadata_dataframe.iloc[i], config=config))
