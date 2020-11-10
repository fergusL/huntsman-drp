import pytest

from huntsman.drp import quality


@pytest.fixture(scope="module")
def filename_list(raw_data_table):
    """
    Load a small amount of data to run the tests on.
    """
    query_dict = dict(dataType="science")
    return raw_data_table.query(query_dict=query_dict)["filename"].values[:2]


def test_metadata_from_fits(filename_list, config):
    """
    Placeholder for a more detailed test.
    """
    mds = []
    for filename in filename_list:
        mds.append(quality.metadata_from_fits(filename, config=config))


def test_raw_quality_table(filename_list, config, raw_quality_table):
    """
    """
    metadata = {}
    for filename in filename_list:
        metadata[filename] = quality.metadata_from_fits(filename, config=config)
        raw_quality_table.insert_one(metadata=metadata[filename])
    query = raw_quality_table.query()
    for _, md in query.iterrows():
        filename = md["filename"]
        assert len(metadata[filename]) == len(md) - 1  # No _id column
        for key, value in metadata[filename].items():
            assert md[key] == value
