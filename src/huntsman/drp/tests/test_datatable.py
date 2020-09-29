import pytest
import copy

from huntsman.drp.fitsutil import read_fits_header
from huntsman.drp.utils import parse_date
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


def test_datatable_insert_many(raw_data_table, test_data):
    raw_data_table.insert_many(test_data['raw_data'])
