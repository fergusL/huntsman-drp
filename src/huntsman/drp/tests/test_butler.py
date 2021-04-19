import os

from huntsman.drp.utils.date import current_date
from huntsman.drp.collection import MasterCalibCollection
from huntsman.drp.utils.testing import create_test_bulter_repository


def test_initialise(butler_repos):
    """Make sure the repos are created properly"""
    for butler_repo in butler_repos:
        with butler_repo:
            for dir in [butler_repo.butler_dir, butler_repo.calib_dir]:
                assert os.path.isdir(dir)
                assert "_mapper" in os.listdir(dir)
            assert butler_repo.get_butler() is not None


def test_temp_repo(temp_butler_repo):
    """Test the temp butler repo behaves as expected"""
    attrs = ["butler_dir", "calib_dir"]
    for a in attrs:
        assert getattr(temp_butler_repo, a) is None
    with temp_butler_repo:
        for a in attrs:
            assert getattr(temp_butler_repo, a) is not None
        assert temp_butler_repo.get_butler() is not None
    # Now check things have been cleaned up properly
    for a in attrs:
        assert getattr(temp_butler_repo, a) is None
        assert len(temp_butler_repo._butlers) == 0


def test_ingest(exposure_table, butler_repos, config):
    """Test ingest for each Butler repository."""
    config = config["exposure_sequence"]
    n_filters = len(config["filters"])

    filenames = exposure_table.find(key="filename")
    for butler_repo in butler_repos:
        with butler_repo as br:

            butler = br.get_butler()

            # Count the number of ingested files
            data_ids = butler.queryMetadata('raw', ['visit', 'ccd'])
            assert len(data_ids) == 0

            br.ingest_raw_data(filenames)
            data_ids = butler.queryMetadata('raw', ['visit', 'ccd'])
            assert len(data_ids) == len(filenames)

            # Check we have the right number of each datatype
            n_flat = config["n_cameras"] * config["n_days"] * config["n_flat"] * n_filters
            data_ids = butler.queryMetadata('raw', ['visit', 'ccd'],
                                            dataId={"dataType": "flat"})
            assert len(data_ids) == n_flat

            n_sci = config["n_cameras"] * config["n_days"] * config["n_science"] * n_filters
            data_ids = butler.queryMetadata('raw', ['visit', 'ccd'],
                                            dataId={"dataType": "science"})
            assert len(data_ids) == n_sci

            n_bias = config["n_cameras"] * config["n_days"] * config["n_bias"]
            data_ids = butler.queryMetadata('raw', ['visit', 'ccd'],
                                            dataId={"dataType": "bias"})
            assert len(data_ids) == n_bias

            n_dark = config["n_cameras"] * config["n_days"] * config["n_dark"] * 2  # 2 exp times
            data_ids = butler.queryMetadata('raw', ['visit', 'ccd'],
                                            dataId={"dataType": "dark"})
            assert len(data_ids) == n_dark


def test_make_master_calibs(exposure_table, temp_butler_repo, config):
    """ Make sure the correct number of master bias frames are produced."""
    test_config = config["exposure_sequence"]
    n_filters = len(test_config["filters"])

    n_bias = test_config["n_cameras"]
    n_dark = test_config["n_cameras"]
    n_flat = test_config["n_cameras"] * n_filters

    # Use the Butler repo to make the calibs
    filenames = exposure_table.find(key="filename")
    with temp_butler_repo as br:
        br.ingest_raw_data(filenames)

        # Make the calibs
        br.make_master_calibs(calib_date=current_date(), rerun="test_rerun")

        # Archive the calibs
        br.archive_master_calibs()

        # Check the biases in the butler dir
        metadata_bias = br.get_calib_metadata(datasetType="bias")
        assert len(metadata_bias) == n_bias
        ccds = set()
        for md in metadata_bias:
            ccds.update([md["ccd"]])
        assert len(ccds) == test_config["n_cameras"]

        # Check the darks in the butler dir
        metadata_dark = br.get_calib_metadata(datasetType="dark")
        assert len(metadata_dark) == n_dark
        ccds = set()
        for md in metadata_dark:
            ccds.update([md["ccd"]])
        assert len(ccds) == test_config["n_cameras"]

        # Check the flats in the butler dir
        metadata_flat = br.get_calib_metadata(datasetType="flat")
        assert len(metadata_flat) == n_flat
        filters = set()
        ccds = set()
        for md in metadata_flat:
            filters.update([md["filter"]])
            ccds.update([md["ccd"]])
        assert len(filters) == 2
        assert len(ccds) == test_config["n_cameras"]

        # Check the calibs in the archive
        master_calib_table = MasterCalibCollection(config=config)
        calib_metadata = master_calib_table.find()
        filenames = [c["filename"] for c in calib_metadata]
        datasettypes = [c["datasetType"] for c in calib_metadata]
        assert len(calib_metadata) == n_flat + n_bias + n_dark
        assert sum([c == "flat" for c in datasettypes]) == n_flat
        assert sum([c == "bias" for c in datasettypes]) == n_bias
        assert sum([c == "dark" for c in datasettypes]) == n_dark
        for filename in filenames:
            assert os.path.isfile(filename)


def test_make_coadd(tmpdir):
    """ Test that we can make coadds. """
    br = create_test_bulter_repository(str(tmpdir))

    br.make_master_calibs(validity=1000)

    br.make_calexps()

    br.make_coadd()  # Implicit verification

    calexps, data_ids = br.get_calexps()
    assert len(calexps) == 1
    assert len(data_ids) == 1
