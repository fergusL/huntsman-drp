from huntsman.drp.fitsutil import read_fits_header
from huntsman.drp.utils import parse_date


def test_query_by_date(raw_data_table, fits_header_translator):
    """ """
    # Get list of all dates in the database
    dates = sorted(raw_data_table.query_column("dateObs"))
    date_max = dates[-1]
    n_files = len(dates)
    for date_min in dates[:-1]:
        # Get filenames between dates
        filenames = raw_data_table.query_column("filename", date_min=date_min, date_max=date_max)
        assert len(filenames) <= n_files  # This holds because we sorted the dates
        n_files = len(filenames)
        for filename in filenames:
            # Assert date is within expected range
            header = read_fits_header(filename)
            date = parse_date(fits_header_translator.translate_dateObs(header))
            assert date >= date_min
            assert date < date_max
