import os
from huntsman.drp.utils.date import date_to_ymd


def get_calib_filename(config, datasetType, **kwargs):
    """
    """
    kwargs["config"] = config

    if datasetType == "bias":
        return get_bias_filename(**kwargs)

    elif datasetType == "dark":
        return get_dark_filename(**kwargs)

    elif datasetType == "flat":
        return get_flat_filename(**kwargs)

    else:
        raise ValueError(f"Unrecognised datasetType: {datasetType}.")


def get_bias_filename(config, calibDate, ccd, **kwargs):
    """
    """
    calibDate = date_to_ymd(calibDate)

    dir = config["directories"]["archive"]
    return os.path.join(dir, "bias", calibDate, f"ccd_{ccd}.fits")


def get_dark_filename(config, calibDate, ccd, **kwargs):
    """
    """
    calibDate = date_to_ymd(calibDate)

    dir = config["directories"]["archive"]
    return os.path.join(dir, "dark", calibDate, f"ccd_{ccd}.fits")


def get_flat_filename(config, calibDate, filter, ccd, **kwargs):
    """
    """
    calibDate = date_to_ymd(calibDate)

    dir = config["directories"]["archive"]
    return os.path.join(dir, "flat", calibDate, f"ccd_{ccd}_filter_{filter}.fits")
