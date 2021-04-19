import os

from lsst.daf.persistence import FsScanner


def get_filename_template(datasetType, policy):
    """ Get the filename template for a specific datatype.
    Args:
        datasetType (str):
            The dataset type as specified in the policy file. e.g. `exposures.raw`
            or `calibrations.flat`.
        policy (`lsst.daf.persistence.Policy`):
            The Policy object.
    Returns:
        str: The filename template.
    """
    policy_key = datasetType + ".template"
    template = policy[policy_key]
    if template is None:
        raise KeyError(f"Template not found for {datasetType}.")
    return template


def get_files_of_type(datasetType, directory, policy):
    """ Get the filenames of a specific dataset type under a particular directory that match
    the appropriate filename template.
    Args:
        datasetType (str): The dataset type as specified in the policy file. e.g. `exposures.raw`
            or `calibrations.flat`.
        directory (str): The directory to search under (e.g. a butler directory).
        policy (`lsst.daf.persistence.Policy`): The Policy object.
    Returns:
        list of dict: The matching data IDs.
        list of str: The matching filenames.
    """
    # Get the filename tempalte for this file type
    template = get_filename_template(datasetType, policy)

    # Find filenames that match the template in the directory
    scanner = FsScanner(template)
    matches = scanner.processPath(directory)
    dataIds = list(matches.values())
    filenames = [os.path.join(directory, f) for f in matches.keys()]

    return dataIds, filenames


def get_all_calibIds(datasetType, dataIds, calibDate, butler):
    """ Get calibIds given a set of dataIds and datasetType.
    Args:
        datasetType (str): The calib type, e.g. flat, bias.
        dataIds (list of dict): The data ids.
        butler (lsst.daf.persistence.butler.Butler): The butler object.
    Returns:
        list of dict: The set of unique calibIds associated with the dataIds.
    """
    # Get required keys
    calib_keys = [k for k in butler.getKeys(datasetType).keys() if k != "calibDate"]

    # Get key values for each dataId
    calib_key_values = []
    for dataId in dataIds:
        calib_key_values.append(tuple([dataId[k] for k in calib_keys]))

    # Get unique sets of calibIds
    unique_calib_values = set(calib_key_values)
    calibIds = [{k: v for k, v in zip(calib_keys, vs)} for vs in unique_calib_values]

    # Add calibDate to calibIds
    for calibId in calibIds:
        calibId["calibDate"] = calibDate

    return calibIds


def calibId_to_dataIds(datasetType, calibId, butler):
    """ Get ingested dataIds that match a calibId of a given datasetType.
    Args:
        datasetType (str): The calib type, e.g. flat, bias.
        CalibId (dict): The calibId.
        butler (lsst.daf.persistence.butler.Butler): The butler object.
    Returns:
        list of dict: A list of matching dataIds.
    """
    raw_keys = list(butler.getKeys("raw"))
    calib_keys = list(butler.getKeys(datasetType))

    # Use these keys to match calibIds to dataIds
    matching_keys = [k for k in raw_keys if k in calib_keys]

    # Get all dataIds inside the butler repo of the correct dataType
    values = butler.queryMetadata("raw", format=raw_keys, dataId={"dataType": datasetType})
    dataIds_all = [{k: v for k, v in zip(raw_keys, vals)} for vals in values]

    # Get matching dataIds
    dataIds = []
    for dataId in dataIds_all:
        if all([dataId[k] == calibId[k] for k in matching_keys]):
            dataIds.append(dataId)

    return dataIds
