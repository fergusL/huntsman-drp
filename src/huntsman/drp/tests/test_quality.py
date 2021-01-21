import pytest

from huntsman.drp import quality


@pytest.fixture(scope="function")
def metadata_dataframe(raw_data_table):
    """ Load a small amount of data to run the tests on. """
    criteria = dict(dataType="science")
    return raw_data_table.query(criteria=criteria)[:2]


def test_metadata_from_fits(metadata_dataframe, config):
    """ Placeholder for a more detailed test. """
    mds = []
    for i in range(metadata_dataframe.shape[0]):
        mds.append(quality.metadata_from_fits(metadata_dataframe.iloc[i], config=config))


def test_raw_quality_table(metadata_dataframe, config, raw_quality_table):
    """
    """
    quality_metadata = {}

    for i in range(metadata_dataframe.shape[0]):
        md = metadata_dataframe.iloc[i]
        filename = md['filename']
        quality_metadata[filename] = quality.metadata_from_fits(md, config=config)
        raw_quality_table.insert(quality_metadata[filename])

    query = raw_quality_table.query()
    for _, md in query.iterrows():
        filename = md["filename"]
        assert len(quality_metadata[filename]) == len(md)
        for key, value in quality_metadata[filename].items():
            assert md[key] == value
