"""Functions to interact with and create calibration data.
"""
from astropy import stats
import astropy.io.fits as fits


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
