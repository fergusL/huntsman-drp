from astropy import stats

METRICS = ("clipped_stats", "flipped_asymmetry")


def clipped_stats(data, file_info, **kwargs):
    """Return sigma-clipped image statistics.

    Parameters
    ----------
    data : array
        Image data as stored as an array.
    file_info : dict
        Dictionary containing image metadata

    Returns
    -------
    dict
        Dictionary containing the calculated stats values.
    """
    mean, median, stdev = stats.sigma_clipped_stats(data)

    # Calculate the well fullness fraction using clipped median
    bit_depth = file_info["BITDEPTH"]
    saturate = 2**bit_depth - 1
    well_fullfrac = median / saturate

    return {"clipped_mean": mean, "clipped_median": median, "clipped_std": stdev,
            "well_fullfrac": well_fullfrac}


def flipped_asymmetry(data, file_info, **kwargs):
    """Calculate the asymmetry statistics by flipping data in x and y directions.

    Parameters
    ----------
    data : array
        Image data as stored as an array.
    file_info : dict
        Dictionary containing image metadata

    Returns
    -------
    dict
        Dictionary containing the calculated stats values.
    """
    # Horizontal flip
    data_flip = data[:, ::-1]
    std_horizontal = (data-data_flip).std()
    # Vertical flip
    data_flip = data[::-1, :]
    std_vertical = (data-data_flip).std()
    return {"flip_asymm_h": std_horizontal, "flip_asymm_v": std_vertical}
