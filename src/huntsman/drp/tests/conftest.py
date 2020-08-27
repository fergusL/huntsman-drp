import os
import pytest
import numpy as np
from astropy.io import fits

from huntsman.drp.fitsutil import FitsHeaderTranslator
from huntsman.drp.metadb import SimulatedMetaDatabase
from huntsman.drp.butler import TemporaryButlerRepository


IMAGES = [dict(dateObs='2020-08-21T09:30:00.000(UTC)', dataType='science'),
          dict(dateObs='2020-08-21T09:30:00.000(UTC)', dataType='science', camera=2),
          dict(dateObs='2020-08-20T09:30:00.000(UTC)', dataType='science'),
          dict(dateObs='2020-08-20T09:30:00.000(UTC)', dataType='science', camera=2),

          dict(dateObs='2020-08-21T08:30:00.000(UTC)', dataType='flat'),
          dict(dateObs='2020-08-21T08:30:00.000(UTC)', dataType='flat', camera=2),
          dict(dateObs='2020-08-20T08:30:00.000(UTC)', dataType='flat'),
          dict(dateObs='2020-08-20T08:30:00.000(UTC)', dataType='flat', camera=2),

          dict(dateObs='2020-08-21T07:30:00.000(UTC)', dataType='bias'),
          dict(dateObs='2020-08-21T07:30:00.000(UTC)', dataType='bias', camera=2),
          dict(dateObs='2020-08-20T07:30:00.000(UTC)', dataType='bias'),
          dict(dateObs='2020-08-20T07:30:00.000(UTC)', dataType='bias', camera=2)]


def make_test_data(filename, dateObs, dataType, camera=1, filter="g2", shape=(30, 50), bias=32,
                   ra=100, dec=-30, exposure_time=30):
    """Make fake FITS images with realistic headers."""
    # Make the fake image data
    if dataType == "science":
        data = np.ones(shape) * bias * 5
        data = np.random.poisson(data) + bias
        field = "A Science Field"
        image_type = "Light Frame"
    elif dataType == "flat":
        data = np.ones(shape, dtype="float32")
        field = "Flat Field"
        image_type = "Light Frame"
    elif dataType == "bias":
        data = bias * np.ones(shape, dtype="uint16")
        field = "Dark Field"
        image_type = "Dark Frame"
    hdu = fits.PrimaryHDU(data)
    # Add the header
    hdu.header["RA"] = ra
    hdu.header["dec"] = dec
    hdu.header['EXPTIME'] = exposure_time
    hdu.header['FILTER'] = filter
    hdu.header['FIELD'] = field
    hdu.header['DATE-OBS'] = dateObs
    hdu.header["IMAGETYP"] = image_type
    hdu.header["INSTRUME"] = f"TESTCAM{camera:02d}"
    hdu.header["IMAGEID"] = "TestImageId"
    # Write as a FITS file
    hdu.writeto(filename, overwrite=True)


@pytest.fixture(scope="session")
def data_directory(tmp_path_factory):
    """Create a temporary directory populated with fake FITS images."""
    tempdir = tmp_path_factory.mktemp("testdata")
    for i, image_dict in enumerate(IMAGES):
        filename = os.path.join(tempdir, f"testdata_{i}.fits")
        make_test_data(filename=filename, **image_dict)
    return tempdir


@pytest.fixture(scope="session")
def metadatabase(data_directory):
    return SimulatedMetaDatabase(data_directory=data_directory, data_info=IMAGES)


@pytest.fixture(scope="session")
def fits_header_translator():
    return FitsHeaderTranslator()


@pytest.fixture(scope="function")
def temp_butler_repo():
    return TemporaryButlerRepository()
