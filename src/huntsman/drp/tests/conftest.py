import os
import pytest
import yaml
import numpy as np
from astropy.io import fits

from huntsman.drp.base import load_config
from huntsman.drp.fitsutil import FitsHeaderTranslator
from huntsman.drp.datatable import RawDataTable
from huntsman.drp.butler import TemporaryButlerRepository


@pytest.fixture(scope="session")
def config():
    return load_config(ignore_local=True)


def make_test_data(filename, taiObs, dataType, camera=1, filter="g2", shape=(30, 50), bias=32,
                   ra=100, dec=-30, exposure_time=30):
    """Make a fake FITS image with a realistic header, returning the header."""
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
    hdu.header['DATE-OBS'] = taiObs
    hdu.header["IMAGETYP"] = image_type
    hdu.header["INSTRUME"] = f"TESTCAM{camera:02d}"
    hdu.header["IMAGEID"] = "TestImageId"
    hdu.header["CCD-TEMP"] = 0
    # Write as a FITS file
    hdu.writeto(filename, overwrite=True)
    return hdu.header


@pytest.fixture(scope="session")
def test_data():
    """List of dictionaries of test data."""
    filename = os.path.join(os.environ["HUNTSMAN_DRP"], "tests", "test_data.yaml")
    with open(filename, 'r') as f:
        data = yaml.safe_load(f)
    return data


@pytest.fixture(scope="session")
def fits_header_translator(config):
    return FitsHeaderTranslator(config=config)


@pytest.fixture(scope="function")
def temp_butler_repo(config):
    return TemporaryButlerRepository(config=config)


@pytest.fixture(scope="session")
def raw_data_table(config, tmp_path_factory, test_data, fits_header_translator):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    raw_data = test_data["raw_data"]
    raw_data_table = RawDataTable(config=config)
    tempdir = tmp_path_factory.mktemp("testdata")
    for i, data_dict in enumerate(raw_data):
        filename = os.path.join(tempdir, f"testdata_{i}.fits")
        # Make the FITS image and get the header
        header = make_test_data(filename=filename, **data_dict)
        # Parse the header
        parsed_header = fits_header_translator.parse_header(header)
        # Insert the parsed header into the DB table
        parsed_header["filename"] = filename
        raw_data_table.insert_one(parsed_header)
    # Make sure table has the correct number of rows
    assert len(raw_data_table.query()) == len(raw_data)
    return raw_data_table
