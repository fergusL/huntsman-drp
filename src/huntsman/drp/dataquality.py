import os
import shutil
from huntsman.drp.metadatabase import MetaDatabase
from huntsman.drp.calibs import make_recent_calibs
from huntsman.drp.calexp import make_calexps, get_calexp_metadata


def make_calexps(workdir):
    """
    Call processCcd.py and record appropriate metadata, e.g.
        - PSF model paramters.
        - Photometric zero point.
        - Background level & RMS.

    Parameters:
        workdir (str): The working directory to mount inside the docker image.
    """
    pass


def get_science_data_quality(data_directory):
    """

    """
    # Query the database for the lastest science files and copy
    mdb = MetaDatabase()
    sciencedir = os.path.join(data_directory, "raw")
    mdb.retrieve_files(directory=sciencedir, data_type="science")

    # Make the daily master calibs
    calibdir = os.path.join(data_directory, "calib")
    make_recent_calibs(output_directory=calibdir)

    # Create the calexps and retrieve metadata
    calexpdir = os.path.join(data_directory, "calexp")
    make_calexps(workdir=data_directory)
    calexp_metadata = get_calexp_metadata()

    # Insert results into database
    for metadata in calexp_metadata:
        mdb.insert(metadata, table="calexp_qc")

    # Clean up directories
    for subdir in [calibdir, sciencedir, calexpdir]:
        shutil.rmtree(subdir)
