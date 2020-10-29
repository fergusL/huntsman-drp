"""Unit tests for calibration data.
"""
import pytest
from datetime import datetime

from huntsman.drp.fitsutil import read_fits_header
from huntsman.drp.utils.date import current_date, parse_date, current_date_ymd


def test_read_fits_header_bad_extension():
    with pytest.raises(ValueError):
        read_fits_header('bogus_file.lala')


def test_parse_date_datetime():
    parse_date(datetime.today())


def test_date_to_ymd():
    date = current_date()
    assert current_date_ymd() == date.strftime('%Y-%m-%d')
