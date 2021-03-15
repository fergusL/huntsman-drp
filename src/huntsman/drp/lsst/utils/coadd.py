"""
See: https://github.com/lsst/pipe_tasks/blob/master/python/lsst/pipe/tasks/makeDiscreteSkyMap.py
"""
from collections import defaultdict


def get_skymap_ids(skymap):
    """ Get the full set of skymap patch IDs from a given skymap.
    Args:
        skymap (lsst.skymap.discreteSkyMap.DiscreteSkyMap): The skymap object.
    Returns:
        dict: A dictionary of tractID: List of patch IDs. Each patch IDs is specified by two
            indices (x and y), in this case they are returned as a single string, `x,y`.
    """
    indices = defaultdict(list)
    for tract_info in skymap:

        # Identify the tract
        tract_id = tract_info.getId()

        # Get lists of x-y patch indices in this tract
        nx = tract_info.getNumPatches()[0]
        ny = tract_info.getNumPatches()[1]
        for x in range(nx):
            for y in range(ny):
                indices[tract_id].append(f"{x},{y}")

    return indices
