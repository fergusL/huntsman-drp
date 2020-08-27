import os
import pytest
from astropy.io import fits
from dateutil.parser import parse as parse_date


def test_query_by_date(metadatabase, fits_header_translator):
    # Get list of all dates in the database
    dates = sorted(metadatabase.query_dates())
    date_max = dates[-1]
    n_files = len(dates)
    for date_min in dates[:-1]:
        # Get filenames between dates
        filenames = metadatabase.query_files(date_min=date_min, date_max=date_max)
        assert len(filenames) <= n_files  # This holds because we sorted the dates
        n_files = len(filenames)
        for filename in filenames:
            # Assert date is within expected range
            header = fits.getheader(filename)
            date = parse_date(fits_header_translator.translate_dateObs(header))
            assert date >= date_min
            assert date < date_max
