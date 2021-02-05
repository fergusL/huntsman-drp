import os
import copy
import json

from lsst.daf.persistence import FsScanner


def get_filename_template(datasetType, policy):
    """ Get the filename template for a specific datatype.
    Args:
        datasetType (str): The dataset type as specified in the policy file. e.g. `exposures.raw`
            or `calibrations.flat`.
        policy (`lsst.daf.persistence.Policy`): The Policy object.
    Returns:
        str: The filename template.
    """
    policy_key = datasetType + ".template"
    template = policy[policy_key]
    if template is None:
        raise KeyError(f"Template not found for {datasetType}.")
    return template


def get_files_of_type(datasetType, directory, policy):
    """
    Get the filenames of a specific dataset type under a particular directory that match
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
    data_ids = list(matches.values())
    filenames = [os.path.join(directory, f) for f in matches.keys()]

    return data_ids, filenames


def fill_calib_keys(data_id, calib_type, butler, keys_ignore=None):
    """ Get missing keys for a dataId required to identify a calib dataset.
    Args:
        data_id (dict): The partial dataId.
        calib_type (str): The type of calib, e.g. bias, flat.
        butler (lsst.daf.persistence.butler.Butler): The butler object.
        keys_ignore (iterable): Required keys to ignore.
    Returns:
        dict: The complete dataId.
    """
    # Get the raw dataId
    data_id = data_id.copy()
    raw_keys = butler.getKeys("raw").keys()
    raw_data_id = {k: data_id[k] for k in raw_keys}

    # Identify required keys
    required_keys = butler.getKeys(calib_type).keys()
    missing_keys = set(required_keys) - set(data_id.keys())
    if keys_ignore is not None:
        missing_keys -= set(keys_ignore)

    # Fill the missing keys
    data_id = data_id.copy()
    for k in missing_keys:
        v = butler.queryMetadata("raw", format=[k], dataId=raw_data_id)
        data_id[k] = v[0]

    return data_id


def get_unique_calib_ids(calib_type, data_ids, butler):
    """ Get calibIds given a set of dataIds for a specific calib type. This is required because
    constrictBias.py and constructCalib.py seem only to be able to handle one calibId at a time.
    Args:
        calib_type (str): The calib type, e.g. flat, bias.
        data_ids (list of dict): The data ids.
        butler (lsst.daf.persistence.butler.Butler): The butler object.
    Returns:
        list of dict: The set of unique calibIds associated with the dataIds.
    """
    data_ids = copy.deepcopy(data_ids)
    # Get required keys
    calib_keys = butler.getKeys(calib_type).keys()

    # Get key values for each dataId
    calib_key_values = []
    for data_id in data_ids:
        calib_key_values.append(tuple([data_id[k] for k in calib_keys]))

    # Get unique sets of calibIds
    unique_calib_values = set(calib_key_values)
    unique_calib_ids = [{k: v for k, v in zip(calib_keys, vs)} for vs in unique_calib_values]

    return unique_calib_ids


def get_missing_data_ids(data_ids, required_data_ids):
    """ Find any data_ids that are not present in data_ids_required. This is tricky as dict
    objects are not hashable and we cannot use the "set" functionality directly. We therefore
    serialise the dicts to str following this method:
    https://stackoverflow.com/questions/11092511/python-list-of-unique-dictionaries
    Args:
        data_ids (list of dict): The dataIds to check.
        required_data_ids (list of dict): The dataIds required to exist in data_ids. Any dataId
            that is not present in data_ids_required is returned.
    Returns:
        list of dict: List of unique data_ids that are not in data_ids_required.
    """
    data_ids_json = set([json.dumps(_, sort_keys=True) for _ in data_ids])
    data_ids_required_json = set([json.dumps(_, sort_keys=True) for _ in required_data_ids])
    missing_ids_json = data_ids_required_json - data_ids_json
    return [json.loads(_) for _ in missing_ids_json]


def data_id_to_calib_id(datasetType, data_ids, butler, keys_ignore=None):
    """ Convert a list of dataIds to corresponding list of calibIds. TODO: Figure out if this
    functionality already exists somewhere in the LSST stack.
    Args:
        datasetType (str): The dataset type, e.g. bias or flat.
        data_ids (list of dict): The dataIds to convert to calibIds.
        keys_ignore (list of str, optional): If given, the returned calibIds will not contain
            any of the keys listed in keys_ignore.
    Returns:
        list of dict: The corresponding calibIds.
    """
    calib_keys = list(butler.getKeys(datasetType).keys())
    if keys_ignore is not None:
        calib_keys = [k for k in calib_keys if k not in keys_ignore]
    calib_ids = [{k: data_id[k] for k in calib_keys} for data_id in data_ids]
    return calib_ids


def get_data_ids(butler, datasetType, dataId=None, extra_keys=None):
    """ Get dataIds for datasetType.
    Args:
        butler (butler): The butler object.
        datasetType (str): The datasetType (raw, bias, flat etc.).
        dataId (dict, optional): A complete or partial dataId to match with.
        extra_keys (list, optional): List of additional keys to be included in the dataIds.
    Returns:
        list of dict: A list of dataIds.
    """
    keys = list(butler.getKeys(datasetType).keys())
    if extra_keys is not None:
        keys.extend(extra_keys)

    value_list = butler.queryMetadata(datasetType, format=keys, dataId=dataId)

    return [{k: v for k, v in zip(keys, _)} for _ in value_list]
