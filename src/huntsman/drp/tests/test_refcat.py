import pytest
import tempfile
import numpy as np
import pandas as pd
from requests.exceptions import HTTPError
from contextlib import suppress

from huntsman.drp import refcat as rc


@pytest.fixture(scope="function")
def ra_list():
    return [30, 40]


@pytest.fixture(scope="function")
def dec_list():
    return [-20, -30]


@pytest.fixture(scope="function")
def testing_refcat_server(config, refcat_filename):
    """ A testing refcat server that loads the refcat from file rather than downloading it.
    """
    refcat_kwargs = dict(refcat_filename=refcat_filename)

    # Yield the refcat server process
    refcat_service = rc.create_refcat_service(pyro_name="test_refcat",
                                              refcat_type=rc.TestingTapReferenceCatalogue,
                                              refcat_kwargs=refcat_kwargs,
                                              config=config)
    refcat_service.start()
    yield refcat_service

    # Shutdown the refcat server after we are done
    refcat_service.stop()


def test_refcat_client(config, testing_refcat_server, ra_list, dec_list):

    ra_key = config["refcat"]["ra_key"]
    dec_key = config["refcat"]["dec_key"]

    client = rc.RefcatClient(pyro_name="test_refcat")

    with tempfile.NamedTemporaryFile() as tf:

        filename = tf.name

        df_ret = client.make_reference_catalogue(ra_list, dec_list, filename=filename)

        for df in df_ret, pd.read_csv(filename):

            assert isinstance(df, pd.DataFrame)
            assert df.shape[0] > 0
            assert ra_key in df.columns
            assert dec_key in df.columns

            # Check that the values are numbers
            df[ra_key].values.astype("float")


def test_make_reference_catalogue(reference_catalogue, config, ra_list, dec_list, tolerance=0.2):

    # Do the cone searches
    try:
        df = reference_catalogue.make_reference_catalogue(ra_list, dec_list)
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
        assert key in df.columns  # Make sure the key is present in df
        with suppress(KeyError):
            assert (df[key].values >= pranges[key]["lower"]).all()
        with suppress(KeyError):
            assert (df[key].values < pranges[key]["upper"]).all()
