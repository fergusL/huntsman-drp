from astropy import stats
import astropy.io.fits as fits

from huntsman.drp.metadb import MetaDatabase
from huntsman.drp.butler import TemporaryButlerRepository


def generate_science_data_quality(meta_database=None, table="calexp_qc"):
    """
    Generate metadata for science data.

    Args:
        meta_database (huntsman.drp.MetaDatabase, optional): The meta database object.
        table (str, optional): The table in which to insert the resulting metadata.
    """
    if meta_database is None:
        meta_database = MetaDatabase()

    # Get filenames of science data to process
    filenames = meta_database.query_recent_files()

    # Create a new butler repo in temp directory
    with TemporaryButlerRepository() as butler_repo:

        # Ingest raw data
        butler_repo.ingest_raw_data(filenames)

        # Make master calibs for today (discarded after use)
        butler_repo.make_master_calibs()

        # Make the calexps
        butler_repo.make_calexps()

        # Get calexp metadata and insert into database
        calexp_metadata = butler_repo.get_calexp_metadata()
        for metadata in calexp_metadata:
            meta_database.insert(metadata, table=table)


def get_simple_image_data_stats(filename_list):
    """Return a dictionary of simple stats for all
    fits filenames in the input list.

    Args:
        filename_list (list): List of fits filenames.

    Returns:
        dict: mean, median, stdev for each fits file
    """
    output_data_quality_dict = {}
    for filename in filename_list:
        mean, median, stdev = stats.sigma_clipped_stats(fits.getdata(filename))
        output_data_quality_dict[filename] = (mean, median, stdev)
    return(output_data_quality_dict)


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
