"""
See: https://github.com/lsst/pipe_tasks/blob/master/python/lsst/pipe/tasks/makeDiscreteSkyMap.py
"""


def get_skymap_ids(skymap):
    """ Get the full set of skymap patch IDs from a given skymap.
    Args:
        skymap (lsst.skymap.discreteSkyMap.DiscreteSkyMap): The skymap object.
    Returns:
        list of dict: A list of dictionaries of tractID: List of patch IDs. Each patch IDs is
            specified by two indices (x and y) returned as a comma-delimited string.
    """
    skymapIds = []

    for tract_info in skymap:

        # Identify the tract
        tractId = tract_info.getId()

        # Get lists of x-y patch indices in this tract
        patchIds = []
        nx = tract_info.getNumPatches()[0]
        ny = tract_info.getNumPatches()[1]
        for x in range(nx):
            for y in range(ny):
                patchIds.append(f"{x},{y}")

        skymapIds.append({"tractId": tractId, "patchIds": patchIds})

    return skymapIds
