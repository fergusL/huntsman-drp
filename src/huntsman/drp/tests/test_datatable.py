import pytest
import copy
from datetime import timedelta

from huntsman.drp.utils import current_date, parse_date
from huntsman.drp.fitsutil import read_fits_header
from huntsman.drp.datatable import RawDataTable

from pymongo.errors import ServerSelectionTimeoutError


def test_mongodb_wrong_host_name(raw_data_table):
    """Test if an error is raised if the mongodb hostname is incorrect."""
    modified_config = copy.copy(raw_data_table.config)
    modified_config["mongodb"]["hostname"] = "nonExistantHostName"
    with pytest.raises(ServerSelectionTimeoutError):
        RawDataTable(config=modified_config)


def test_datatable_query_by_date(raw_data_table, fits_header_translator):
    """ """
    # Get list of all dates in the database
    dates = sorted(raw_data_table.query_column("dateObs"))
    date_end = dates[-1]
    n_files = len(dates)
    for date_start in dates[:-1]:
        # Get filenames between dates
        filenames = raw_data_table.query_column("filename", date_start=date_start,
                                                date_end=date_end)
        assert len(filenames) <= n_files  # This holds because we sorted the dates
        n_files = len(filenames)
        for filename in filenames:
            # Assert date is within expected range
            header = read_fits_header(filename)
            date = parse_date(fits_header_translator.translate_dateObs(header))
            assert date >= parse_date(date_start)
            assert date < parse_date(date_end)


def test_query_latest(raw_data_table, config, tol=1):
    """Test query_latest finds the correct number of DB entries."""
    date_start = config["testing"]["exposure_sequence"]["start_date"]
    n_days = config["testing"]["exposure_sequence"]["n_days"]
    date_start = parse_date(date_start)
    date_now = current_date()
    if date_now <= date_start + timedelta(days=n_days):
        pytest.skip(f"Test does not work unless current date is later than all test exposures.")
    timediff = date_now - date_start
    # This should capture all the files
    qresult = raw_data_table.query_latest(days=timediff.days + tol)
    assert len(qresult) == len(raw_data_table.query())
    # This should capture none of the files
    qresult = raw_data_table.query_latest(days=0, hours=0, seconds=0)
    assert len(qresult) == 0
