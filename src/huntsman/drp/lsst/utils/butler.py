from lsst.daf.persistence.policy import Policy


def load_policy():
    """ Load the LSST policy.
    Returns:
        lsst.daf.persistence.policy.Policy: The policy object.
    """
    policy_filename = Policy.defaultPolicyFile("obs_huntsman", "HuntsmanMapper.yaml",
                                               relativePath="policy")
    return Policy(policy_filename)


def get_filename_template(datasetType):
    """ Get the filename template for a specific datatype.
    Args:
        datasetType (str):
            The dataset type as specified in the policy file. e.g. `exposures.raw`
            or `calibrations.flat`.
    Returns:
        str: The filename template.
    """
    policy = load_policy()
    policy_key = datasetType + ".template"

    template = policy[policy_key]
    if template is None:
        raise KeyError(f"Template not found for {datasetType}.")

    return template


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
