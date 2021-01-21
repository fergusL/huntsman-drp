"""
Functions to calculate data quality metrics.
"""
from astropy.io import fits
from astropy import stats

from huntsman.drp.core import get_logger

METRICS = "clipped_stats", "flipped_asymmetry"  # TODO: Refactor!
QUALITY_FLAG_NAME = "quality_success_flag"


def metadata_from_fits(file_info, config=None, logger=None, dtype="float32"):
    """ Return a dictionary of data quality metrics for the file. A flag is added to indicate if
    all metics were calculated successfully.
    Args:
        file_info (abc.Mapping): Filename of FITS image.
        dtype (str or Type): Convert the image data to this type before processing.
    Returns:
        dict: A dictionary of metadata key: value pairs, including the filename.
    """
    if logger is None:
        logger = get_logger()
    filename = file_info["filename"]

    logger.debug(f"Calculating data quality metrics for {filename}.")
    result = {"filename": filename, QUALITY_FLAG_NAME: True}

    # Load the data from file
    try:
        data = fits.getdata(filename).astype(dtype)
    except Exception as err:  # Data may be missing or corrupt, so catch all errors here
        logger.error(f"Unable to read file {filename}: {err}")
        result[QUALITY_FLAG_NAME] = False
        return result

    # Calculate metrics
    for metric_name in METRICS:
        logger.debug(f"Calcualating metric for {filename}: {metric_name}.")
        try:
            result.update(globals()[metric_name](data, file_info, config=config, logger=logger))
        except Exception as err:
            logger.error(f"Problem getting '{metric_name}' metric for {filename}: {err}")
            result[QUALITY_FLAG_NAME] = False

    return result


def clipped_stats(data, file_info, logger, **kwargs):
    """ Return sigma-clipped image statistics. """
    mean, median, stdev = stats.sigma_clipped_stats(data)

    # Calculate the well fullness fraction using clipped median
    bit_depth = file_info["BITDEPTH"]
    saturate = 2**bit_depth - 1
    well_fullfrac = median / saturate

    return {"clipped_mean": mean, "clipped_median": median, "clipped_std": stdev,
            "well_fullfrac": well_fullfrac}


def flipped_asymmetry(data, file_info, **kwargs):
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
