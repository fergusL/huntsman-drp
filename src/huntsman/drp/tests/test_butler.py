
import os

def test_initialise(butler_repos):
    """Make sure the repos are created properly"""
    for butler_repo in butler_repos:
        with butler_repo:
            for dir in [butler_repo.butler_directory, butler_repo.calib_directory]:
                assert os.path.isdir(dir)
                assert "_mapper" in os.listdir(dir)
            assert butler_repo.butler is not None


def test_temp_repo(temp_butler_repo):
    """Test the temp butler repo behaves as expected"""
    attrs = ["butler", "butler_directory", "calib_directory"]
    for a in attrs:
        assert getattr(temp_butler_repo, a) is None
    with temp_butler_repo:
        for a in attrs:
            assert getattr(temp_butler_repo, a) is not None
    # Now check things have been cleaned up properly
    for a in attrs:
        assert getattr(temp_butler_repo, a) is None


def test_ingest(raw_data_table, butler_repos):
    """Test ingest for each Butler repository."""
    filenames = raw_data_table.query_column("filename")
    for butler_repo in butler_repos:
        with butler_repo as br:
            data_ids = br.butler.queryMetadata('raw', ['visit', 'ccd'])
            assert len(data_ids) == 0
            br.ingest_raw_data(filenames)
            data_ids = br.butler.queryMetadata('raw', ['visit', 'ccd'])
            assert len(data_ids) == len(filenames)
