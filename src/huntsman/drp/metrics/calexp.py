""" Some parts of the code are adapted from the LSST stack club:
https://nbviewer.jupyter.org/github/LSSTScienceCollaborations/StackClub/blob/rendered/Validation/image_quality_demo.nbconvert.ipynb
"""
import numpy as np
from astropy import units as u

from lsst.afw.geom.ellipses import Quadrupole, SeparableDistortionTraceRadius

from huntsman.drp.core import get_logger
from huntsman.drp.utils.date import current_date
from huntsman.drp.utils.library import load_module


METRICS = ("zeropoint", "psf", "background", "sourcecat")


def calculate_metrics(task_result, metrics=METRICS, logger=None):
    """ Evaluate metrics for a single calexp.
    Args:
        calexp: The LSST calexp object.
        metrics (list of str): list of metrics to calculate.
    Returns:
        dict: A dictionary of metric name: value.
    """
    if logger is None:
        logger = get_logger()

    # Record date of modification
    result = {"date_modified": current_date()}

    for key in ("isrSuccess", "charSuccess", "calibSuccess"):
        result[key] = task_result[key]

    for func_name in metrics:

        func = load_module(f"huntsman.drp.metrics.calexp.{func_name}")

        try:
            metric_dict = func(task_result)
        except Exception as err:
            logger.error(f"Exception while calculation {func_name} metric: {err!r}")
            continue

        for k, v in metric_dict.items():
            if k in result:
                raise KeyError(f"Key '{k}' already in metrics dict.")
            result[k] = v

    return result


def background(task_result):
    """ Calculate sky background statistics.
    Args:
        task_result (dict): The result of ProcessCcdTask.
    Returns:
        dict: The dictionary of results
    """
    result = {}

    # Background from image characterisation
    if task_result["charSuccess"]:
        bg = task_result["charRes"].background.getImage().getArray()
        result["bg_median_char"] = np.median(bg)
        result["bg_std_char"] = bg.std()

    # Background from final calibrated image
    if task_result["calibSuccess"]:
        bg = task_result["calibRes"].background.getImage().getArray()
        result["bg_median"] = np.median(bg)
        result["bg_std"] = bg.std()

    return result


def sourcecat(task_result):
    """ Metadata from source catalogue.
    Args:
        task_result (dict): The result of ProcessCcdTask.
    Returns:
        dict: The dictionary of results
    """
    result = {}

    # Count the number of sources used for image characterisation
    if task_result["charSuccess"]:
        result["n_src_char"] = len(task_result["charRes"].sourceCat)

    # Count the number of sources in the final catalogue
    if task_result["calibSuccess"]:
        result["n_src"] = len(task_result["calibRes"].sourceCat)

    return result


def zeropoint(task_result):
    """ Get the magnitude zero point of the raw data.
    Args:
        calexp (lsst.afw.image.exposure): The calexp object.
    Returns:
        dict: Dict containing the zeropoint in mags.
    """
    if not task_result["calibSuccess"]:
        return {}

    # Find number of sources used for photocal
    n_sources = sum(task_result["calibRes"].sourceCat["calib_photometry_used"])

    calexp = task_result["exposure"]
    pc = calexp.getPhotoCalib()

    # Get the magnitude zero point
    zp_flux = pc.getInstFluxAtZeroMagnitude()
    zp_mag = 2.5 * np.log10(zp_flux) * u.mag  # Note the missing minus sign here...

    # Record calibration uncertainty
    # See: https://hsc.mtk.nao.ac.jp/pipedoc/pipedoc_7_e/tips_e/mag_zeropoint.html
    zp_flux_err = zp_flux * pc.getCalibrationErr() / pc.getCalibrationMean()

    return {"zp_mag": zp_mag, "zp_flux": zp_flux, "zp_flux_err": zp_flux_err,
            "zp_n_src": n_sources}


def psf(task_result):
    """ Calculate PSF metrics.
    This formula (based on a code shared in the stack club) assumes a Gaussian PSF, so the returned
    FWHM is an approximation that can be used to monitor data quality.
    Args:
        calexp (lsst.afw.image.exposure): The calexp object.
    Returns:
        dict: Dict containing the PSF FWHM in arcsec and ellipticity.
    """
    if not task_result["charSuccess"]:
        return {}
    if not task_result["charRes"].psfSuccess:
        return {"psfSuccess": False}

    # Find number of sources used to measure PSF
    n_sources = sum(task_result["charRes"].sourceCat["calib_psf_used"])

    calexp = task_result["exposure"]

    psf = calexp.getPsf()
    shape = psf.computeShape()  # At the average position of the stars used to measure it

    # PSF FWHM (assumes Gaussian PSF)
    pixel_scale = calexp.getWcs().getPixelScale().asArcseconds()
    fwhm = 2 * np.sqrt(2. * np.log(2)) * shape.getTraceRadius() * pixel_scale

    # PSF ellipticity
    i_xx, i_yy, i_xy = shape.getIxx(), shape.getIyy(), shape.getIxy()
    q = Quadrupole(i_xx, i_yy, i_xy)
    s = SeparableDistortionTraceRadius(q)
    e1, e2 = s.getE1(), s.getE2()
    ell = np.sqrt(e1 ** 2 + e2 ** 2)

    return {"psf_fwhm_arcsec": fwhm * u.arcsecond, "psf_ell": ell, "psf_n_src": n_sources,
            "psfSuccess": True}
