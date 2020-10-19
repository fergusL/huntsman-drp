"""
Functions to calculate data quality metrics.
"""
from astropy.io import fits
from astropy import stats

METRICS = "clipped_stats", "flipped_asymmetry"


def metadata_from_fits(filename, config=None):
    """
    Return a dictionary of simple image stats for the file.
    Args:
        filename (str): Filename of FITS image.
    Returns:
        dict: A dictionary of metadata key: value pairs.
    """
    data = fits.getdata(filename)
    result = dict(filename=filename)
    for metric_name in METRICS:
        result.update(globals()[metric_name](data, config=config))
    return result


def clipped_stats(data, **kwargs):
    """ Return sigma-clipped image statistics. """
    mean, median, stdev = stats.sigma_clipped_stats(data)
    return {"clipped_mean": mean, "clipped_median": median, "clipped_std": stdev}


def flipped_asymmetry(data, **kwargs):
    """
    Calculate the asymmetry statistics by flipping data in x and y directions.
    """
    # Horizontal flip
    data_flip = data[:, ::-1]
    std_horizontal = (data-data_flip).std()
    # Vertical flip
    data_flip = data[::-1, :]
    std_vertical = (data-data_flip).std()
    return {"flip_asymm_h": std_horizontal, "flip_asymm_v": std_vertical}
