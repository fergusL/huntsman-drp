"""Unit tests for calibration data.
"""
import pytest
from huntsman.drp.dataquality import get_simple_image_data_stats
#from .conftest import make_fake_image
#from ..utils import get_simple_image_data_stats


@pytest.fixture(scope="module")
def fits_filename_list(metadatabase):
    """A fixture for generating a list of newly-created
    fake fits image calibration data.

    Args:
        tmpdir (Localpath): A temp working directory.

    Returns:
        list(str): List of fits filenames.
    """
    meta_flat = metadatabase.query(dataType="flat")
    meta_bias = metadatabase.query(dataType="bias")
    filenames_flat = [m["filename"] for m in meta_flat]
    filenames_dark = [m["filename"] for m in meta_bias]
    return(filenames_flat + filenames_dark)


@pytest.mark.skip
def test_get_simple_image_data_stats(fits_filename_list):
    """Test get_simple_image_data_stats function.

    Args:
        fits_filename_list (list): List of fits filenames.
    """
    data_quality_dict = get_simple_image_data_stats(fits_filename_list)

    for k in data_quality_dict.keys():
        if k.replace('flat', '') != k:
            assert data_quality_dict[k][0] == pytest.approx(10000, 10)
        else:
            assert data_quality_dict[k][0] == pytest.approx(5, 2)
