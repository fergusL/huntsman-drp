import os
from collections import defaultdict

from huntsman.drp.utils.date import current_date
from huntsman.drp.collection import MasterCalibCollection
from huntsman.drp.lsst.butler import ButlerRepository, TemporaryButlerRepository


def test_initialise(config, tmp_path_factory):
    """Make sure the repos are created properly"""

    dir = tmp_path_factory.mktemp("fixed_butler_repo")

    butler_repos = (TemporaryButlerRepository(config=config),
                    ButlerRepository(directory=str(dir), config=config))

    for butler_repo in butler_repos:
        with butler_repo:
            for dir in [butler_repo.butler_dir, butler_repo.calib_dir]:
                assert os.path.isdir(dir)
                assert "_mapper" in os.listdir(dir)
            assert butler_repo.get_butler() is not None


def test_temp_repo(config):
    """Test the temp butler repo behaves as expected"""

    br = TemporaryButlerRepository(config=config)

    attrs = ["butler_dir", "calib_dir"]
    for a in attrs:
        assert getattr(br, a) is None

    with br:
        for a in attrs:
            assert getattr(br, a) is not None
        assert br.get_butler() is not None

    # Now check things have been cleaned up properly
    for a in attrs:
        assert getattr(br, a) is None
        assert len(br._butlers) == 0


def test_ingest(exposure_collection,  config):
    """Test ingest for each Butler repository."""

    br = TemporaryButlerRepository(config=config)

    config = config["exposure_sequence"]  # TODO: Rename
    n_filters = len(config["filters"])

    filenames = exposure_collection.find(key="filename")

    with br:

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


def test_make_master_calibs(exposure_collection, config):
    """ Make sure the correct number of master bias frames are produced. """

    test_config = config["exposure_sequence"]

    # Specify subset of ingested files to process
    # Here we process a single night for a single camera
    doc = exposure_collection.find()[0]
    doc_filter = {k: doc[k] for k in ["CAM-ID", "dateObs"]}
    docs = exposure_collection.find(doc_filter)

    n_cameras = 1
    n_days = 1
    n_filters = len(test_config["filters"])

    # Check we have the expected number of raw calib files
    # n_bias = n_cameras * n_days * n_bias_per_day
    n_raw_bias = n_cameras * n_days * test_config["n_bias"]
    assert n_raw_bias == len([d for d in docs if d["dataType"] == "bias"])

    # n_dark = n_cameras * n_days * n_dark_per_day * n_unique_exptimes
    n_raw_dark = n_cameras * n_days * test_config["n_dark"] * 2
    assert n_raw_dark == len([d for d in docs if d["dataType"] == "dark"])

    # n_flat = n_cameras * n_days * n_flat_per_day * n_filters
    n_raw_flat = n_cameras * n_days * test_config["n_flat"] * n_filters
    assert n_raw_flat == len([d for d in docs if d["dataType"] == "flat"])

    # Specify the expected number of output master calibs
    n_bias = n_cameras
    n_dark = n_cameras
    n_flat = n_cameras * n_filters

    filenames = [d["filename"] for d in docs]

    with TemporaryButlerRepository(config=config) as br:

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
        assert len(ccds) == n_cameras

        # Check the darks in the butler dir
        metadata_dark = br.get_calib_metadata(datasetType="dark")
        assert len(metadata_dark) == n_dark
        ccds = set()
        for md in metadata_dark:
            ccds.update([md["ccd"]])
        assert len(ccds) == n_cameras

        # Check the flats in the butler dir
        metadata_flat = br.get_calib_metadata(datasetType="flat")
        assert len(metadata_flat) == n_flat
        filters = set()
        ccds = set()
        for md in metadata_flat:
            filters.update([md["filter"]])
            ccds.update([md["ccd"]])
        assert len(filters) == n_filters
        assert len(ccds) == n_cameras

        # Check the calibs in the archive
        master_calib_collection = MasterCalibCollection(config=config)
        calib_metadata = master_calib_collection.find()
        filenames = [c["filename"] for c in calib_metadata]
        datasettypes = [c["datasetType"] for c in calib_metadata]
        assert len(calib_metadata) == n_flat + n_bias + n_dark
        assert sum([c == "flat" for c in datasettypes]) == n_flat
        assert sum([c == "bias" for c in datasettypes]) == n_bias
        assert sum([c == "dark" for c in datasettypes]) == n_dark
        for filename in filenames:
            assert os.path.isfile(filename)


def test_make_coadd(exposure_collection_real_data, master_calib_collection_real_data,
                    refcat_filename, config):
    """ Test that we can make coadds """

    # Identify science files to process
    docs = exposure_collection_real_data.find({"dataType": "science"})
    assert len(docs) > 0

    # Get corresponding calibs
    calib_filenames = defaultdict(list)
    for doc in docs:
        calibs = master_calib_collection_real_data.get_matching_calibs(doc)
        for datasetType, calib_doc in calibs.items():
            calib_filenames[datasetType].append(calib_doc["filename"])

    with TemporaryButlerRepository(config=config) as br:

        # Ingest raw data
        br.ingest_raw_data([d["filename"] for d in docs])

        # Ingest raw calibs
        for datasetType, filenames in calib_filenames.items():
            br.ingest_master_calibs(datasetType, filenames)

        # Ingest refcat
        br.ingest_reference_catalogue([refcat_filename])

        # Make the calexp
        br.make_calexps()

        # Assemble coadd
        br.make_coadd()  # Implicit verification

        calexps, data_ids = br.get_calexps()
        assert len(calexps) == 1
        assert len(data_ids) == 1
