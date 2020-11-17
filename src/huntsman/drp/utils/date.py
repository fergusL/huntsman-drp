"""Functionality to assist handling of dates within huntsman-drp."""
from contextlib import suppress
from datetime import datetime
from dateutil.parser import parse as parse_date_dateutil

import pandas as pd


def parse_date(date):
    """ Parse a date as a `datetime.datetime`.
    Args:
        date (Object): The object to parse.
    Returns:
        A `datetime.datetime` object.
    """
    if isinstance(date, int):
        return datetime.fromtimestamp(date / 1e3)
    if isinstance(date, pd.Timestamp):
        return datetime.fromtimestamp(date)
    with suppress(AttributeError):
        date = date.strip("(UTC)")
    if type(date) is datetime:
        return date
    return parse_date_dateutil(date)


def date_to_ymd(object):
    """ Convert a date to YYYY:MM:DD format.
    Args:
        object (Object): An object that can be parsed using `parse_date`.
    Returns:
        str: The converted date.
    """
    date = parse_date(object)
    return date.strftime('%Y-%m-%d')


def current_date():
    """Returns the UTC time now as a `datetime.datetime` object."""
    return datetime.utcnow()


def current_date_ymd():
    """ Get the UTC date now in YYYY-MM-DD format.
    Returns:
        str: The date.
    """
    date = current_date()
    return date_to_ymd(date)
