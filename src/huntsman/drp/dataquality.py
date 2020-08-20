import os
import shutil
from huntsman.drp.metadatabase import MetaDatabase
from huntsman.drp.calibs import make_recent_calibs
from huntsman.drp.calexp import make_calexps, get_calexp_metadata
from huntsman.drp.utils import get_simple_image_data_stats


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


def generate_science_data_quality(mdb,
                                  date_min,
                                  date_max):
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


def generate_calib_data_quality(mdb,
                                date_min,
                                date_max):
    """Populate meta DB with data quality metrics for
    calibration data taken over the given date range.

    Args:
        mdb (MetaDatabase): Instance of the meta DB.
        date_min (datetime): Start of date to query.
        date_max (datetime): End of date to query
    """
    filename_list = mdb.retrieve_files(data_type="calib",
                                       date_min=date_min,
                                       date_max=date_max)

    stats_dict = get_simple_image_data_stats(filename_list)

    mdb.ingest_calib_stats(stats_dict)
