import os

from huntsman.drp.core import get_config


def get_calib_filename(document, config=None, directory=None):
    """ Return the archived calib filename for a calib dataId.
    Args:
        document (abc.Mapping): The mapping with necessary metadata to construct the filename.
        directory (str, optional): The root directory of the calib. If None, use archive dir from
            config.
        config (dict, optional): The config. If None (default), will get default config.
    Returns:
        str: The archived calib filename.
    """
    # Import here to avoid ImportError with screener
    from huntsman.drp.lsst.utils.butler import get_filename_template

    if directory is None:
        if not config:
            config = get_config()
        directory = config["directories"]["archive"]

    # Load the filename template and get the filename
    key = f"calibrations.{document['datasetType']}"
    filename = get_filename_template(key) % document

    return os.path.join(directory, filename)
