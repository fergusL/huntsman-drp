import pytest
import numpy as np
from requests.exceptions import HTTPError
from contextlib import suppress


def test_create_refcat(reference_catalogue, config, tolerance=0.2):

    # Do the cone searches
    ra_list = [30, 40]  # Make sure these aren't near RA=0
    dec_list = [-20, -30]
    try:
        df = reference_catalogue.create_refcat(ra_list, dec_list)
    except HTTPError as err:
        pytest.skip(f"Encountered HTTPError while testing refcat: {err}")
    assert df.shape[0] != 0  # Make sure we have some sources

    # Make sure the required columns are present
    ra_key = config["refcat"]["ra_key"]
    dec_key = config["refcat"]["dec_key"]
    object_key = config["refcat"]["unique_source_key"]
    for key in (ra_key, dec_key, object_key):
        assert key in df.columns

    # Check all sources are inside cone search radius
    radius = config["refcat"]["cone_search_radius"]
    inside_cone = np.zeros(df.shape[0], dtype="bool")
    ra_ref, dec_ref = df[ra_key].values, df[dec_key].values
    for ra, dec in zip(ra_list, dec_list):
        inside_cone |= (ra_ref - ra)**2 - (dec_ref - dec)**2 <= (radius + tolerance)**2
    assert inside_cone.all()

    # Ensure parameters are within ranges
    pranges = config["refcat"]["parameter_ranges"]
    for key in pranges:
        assert key in df.columns # Make sure the key is present in df
        with suppress(KeyError):
            assert (df[key].values >= pranges[key]["lower"]).all()
        with suppress(KeyError):
            assert (df[key].values < pranges[key]["upper"]).all()
