import pytest
import copy
from datetime import timedelta
import numpy as np

from huntsman.drp.utils.date import current_date, parse_date
from huntsman.drp.fitsutil import read_fits_header
from huntsman.drp.datatable import ExposureTable

from pymongo.errors import ServerSelectionTimeoutError


def test_mongodb_wrong_host_name(exposure_table, config):
    """Test if an error is raised if the mongodb hostname is incorrect."""
    modified_config = copy.deepcopy(config)
    modified_config["mongodb"]["hostname"] = "nonExistantHostName"
    with pytest.raises(ServerSelectionTimeoutError):
        ExposureTable(config=modified_config)


def test_datatable_query_by_date(exposure_table, fits_header_translator):
    """ """
    # Get list of all dates in the database
    dates = [d["dateObs"] for d in exposure_table.find()]
    n_files = len(dates)

    dates_unique = np.unique(dates)  # Sorted array of unique dates
    date_end = dates_unique[-1]
    for date_start in dates_unique[:-1]:
        # Get filenames between dates
        filenames = exposure_table.find(key="filename", date_start=date_start, date_end=date_end)
        assert len(filenames) <= n_files  # This holds because we sorted the dates
        n_files = len(filenames)
        for filename in filenames:
            # Assert date is within expected range
            header = read_fits_header(filename)
            date = parse_date(fits_header_translator.translate_dateObs(header))
            assert date >= parse_date(date_start)
            assert date < parse_date(date_end)


def test_query_latest(exposure_table, config, tol=1):
    """Test query_latest finds the correct number of DB entries."""
    date_start = config["exposure_sequence"]["start_date"]
    n_days = config["exposure_sequence"]["n_days"]
    date_start = parse_date(date_start)
    date_now = current_date()
    if date_now <= date_start + timedelta(days=n_days):
        pytest.skip(f"Test does not work unless current date is later than all test exposures.")
    timediff = date_now - date_start
    # This should capture all the files
    qresult = exposure_table.find_latest(days=timediff.days + tol)
    assert len(qresult) == len(exposure_table.find())
    # This should capture none of the files
    qresult = exposure_table.find_latest(days=0, hours=0, seconds=0)
    assert len(qresult) == 0


def test_update(exposure_table):
    """Test that we can update a document specified by a filename."""
    data = exposure_table.find()[0]
    # Get a filename to use as an identifier
    filename = data["filename"]
    # Get a key to update
    key = [_ for _ in data.keys() if _ not in ["filename", "_id"]][0]
    old_value = data[key]
    new_value = "ThisIsAnewValue"
    assert old_value != new_value  # Let's be sure...
    # Update the key with the new value
    update_dict = {key: new_value, "filename": filename}
    exposure_table.update_one(data, update_dict)
    # Check the values match
    data_updated = exposure_table.find()[0]
    assert data_updated["_id"] == data["_id"]
    assert data_updated[key] == new_value


def test_update_file_data_bad_filename(exposure_table):
    """Test that we can update a document specified by a filename."""
    # Specify the bad filename
    filenames = exposure_table.find(key="filename")
    filename = "ThisFileDoesNotExist"
    assert filename not in filenames
    update_dict = {"A Key": "A Value", "filename": filename}
    with pytest.raises(RuntimeError):
        exposure_table.update_one(update_dict, update_dict, upsert=False)
