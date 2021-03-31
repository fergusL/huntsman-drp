"""
Functions to calculate data quality metrics.
"""
import os

from astropy.io import fits

from huntsman.drp.core import get_logger

METRICS = "clipped_stats", "flipped_asymmetry"  # TODO: Refactor!
QUALITY_FLAG_NAME = "quality_success_flag"


def screen_success(document):
    """ Test if the file has passed screening.
    Args:
        document (dict): The document for the file.
    Returns:
        bool: True if success, else False.
    """
    try:
        return bool(document["quality"]["screen_success"])
    except KeyError:
        return False


def recursively_list_fits_files_in_directory(directory):
    """Returns list of all files contained within a top level directory, including files
    within subdirectories.

    Args:
        directory (str): Directory to examine.
    """
    # create a list of fits files within the directory of interest
    files_in_directory = []
    for dirpath, dirnames, filenames in os.walk(directory):
        for file in filenames:
            # append the filepath to fpaths if file is a fits or fits.fz file
            if file.endswith('.fits') or file.endswith('.fits.fz'):
                files_in_directory.append(os.path.join(dirpath, file))
    return files_in_directory


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

    # Calculate metrics
    for metric_name in METRICS:
        logger.debug(f"Calcualating metric for {filename}: {metric_name}.")
        try:
            result.update(globals()[metric_name](data, file_info, config=config, logger=logger))
        except Exception as err:
            logger.error(f"Problem getting '{metric_name}' metric for {filename}: {err}")
            result[QUALITY_FLAG_NAME] = False
