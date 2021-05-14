import os
from lsst.obs.huntsman import HuntsmanMapper

from huntsman.drp.core import get_config


def get_calib_filename(datasetType, dataId, config=None, mapper=None):
    """ Return the archived filename for a calib dataId.
    Args:
        datasetType (str): The datasetType (e.g. bias).
        dataId (dict): The dataId of the calib.
        config (dict, optional): The config. If None (default), will get default config.
        mapper (lsst.daf.persistence.mapper.Mapper): The mapper object. If None (default), use the
            default for the obs package. Providing the mapper can speed up runtime since it takes
            a while to create the mapper object.
    Returns:
        str: The archived calib filename.
    """
    if not config:
        config = get_config()
    archive_dir = config["directories"]["archive"]

    # The policy yaml file defines the file naming convention
    # The mapper reads the policy file and turns dataIds into ButlerLocation objects
    if mapper is None:
        mapper = HuntsmanMapper()

    filename_list = getattr(mapper, f"map_{datasetType}_filename")(dataId=dataId).locationList
    if len(filename_list) > 1:
        raise RuntimeError("dataId matches with multiple locations.")

    return os.path.join(archive_dir, filename_list[0])
