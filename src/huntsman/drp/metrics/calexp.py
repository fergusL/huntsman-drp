import numpy as np
from astropy import units as u

METRICS = ("mag_zeropoint", "psf_fwhm")


def mag_zeropoint(calexp):
    """ Get the magnitude zero point of the raw data.
    Args:
        calexp (lsst.afw.image.exposure): The calexp object.
    Returns:
        astropy.Quantity: The magnitude zero point.
    """
    fluxzero = calexp.getPhotoCalib().getInstFluxAtZeroMagnitude()
    # Note the missing minus sign here...
    return 2.5 * np.log10(fluxzero) * u.mag


def psf_fwhm(calexp):
    """ Calculate the PSF FWHM.
    This formula (based on a code shared in the stack club) assumes a Gaussian PSF, so the returned
    FWHM is an approximation that can be used to monitor data quality.
    Args:
        calexp (lsst.afw.image.exposure): The calexp object.
    Returns:
        astropy.Quantity: The approximate PSF FWHM.
    """
    psf = calexp.getPsf()
    pixel_scale = calexp.getWcs().getPixelScale().asArcseconds()
    fwhm = 2 * np.sqrt(2. * np.log(2)) * psf.computeShape().getTraceRadius() * pixel_scale
    return fwhm * u.arcsec
