import os
import pytest

from huntsman.drp.reduction import create_from_file


@pytest.fixture()
def config_file_lsst(config):
    rootdir = config["directories"]["root"]
    return os.path.join(rootdir, "config", "reductions", "test-lsst.yaml")


def test_lsst_reduction(exposure_collection_real_data, master_calib_collection_real_data, config,
                        config_file_lsst, testing_refcat_server):

    # TODO: Implement more rigorous test in future

    reduction = create_from_file(config_file_lsst,
                                 exposure_collection=exposure_collection_real_data,
                                 calib_collection=master_calib_collection_real_data,
                                 config=config)

    reduction.run(makeplots=True)
