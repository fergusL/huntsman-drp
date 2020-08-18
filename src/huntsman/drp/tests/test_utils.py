"""Unit tests for calibration data.
"""
import pytest
from .conftest import make_fake_image
from ..utils import get_simple_image_data_stats


@pytest.fixture
def fits_filename_list(tmpdir):
    """A fixture for generating a list of newly-created
    fake fits image calibration data.

    Args:
        tmpdir (Localpath): A temp working directory.

    Returns:
        list(str): List of fits filenames.
    """
    flatfilenames = make_fake_image(tmpdir, 'flat', num_images=1, background=10000)[1]
    darkfilenames = make_fake_image(tmpdir, 'dark', num_images=1, n_sources=0, background=5)[1]
    return(flatfilenames + darkfilenames)


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
