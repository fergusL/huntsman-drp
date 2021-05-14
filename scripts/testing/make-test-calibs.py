""" Script to make master calibs from testdata.
This should not have to be run regularly, but only after e.g. upgrading the LSST stack, modifying
the testing data, or changing the LSST calib policy.
"""
import os
import shutil

from huntsman.drp.services.calib import MasterCalibMaker
from huntsman.drp.collection import MasterCalibCollection
from huntsman.drp.utils.testing import create_test_exposure_collection


if __name__ == "__main__":

    # Make an exposure collection with just the test data
    raw = create_test_exposure_collection()

    # Make a master calib collection just for the test data
    calib = MasterCalibCollection(collection_name="calib-test")

    # Make the calibs
    calib_maker = MasterCalibMaker(exposure_collection=raw, calib_collection=calib)
    for date in calib_maker._get_unique_dates():
        calib_maker.process_date(date)

    # Copy files from the archive into the test data dir
    idir = os.path.join(calib_maker.config["directories"]["archive"], "calib")
    odir = os.path.join(calib_maker.config["directories"]["root"], "tests", "data", "calib")

    shutil.copytree(idir, odir)
