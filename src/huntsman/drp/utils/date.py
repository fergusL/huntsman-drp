"""Functionality to assist handling of dates within huntsman-drp."""
from contextlib import suppress
from datetime import datetime
from dateutil.parser import parse as parse_date_dateutil


def parse_date(object):
    """
    Parse a date as a `datetime.datetime`.

    Args:
        object (Object): The object to parse.

    Returns:
        A `datetime.datetime` object.
    """
    with suppress(AttributeError):
        object = object.strip("(UTC)")
    if type(object) is datetime:
        return object
    return parse_date_dateutil(object)


def date_to_ymd(object):
    """
    Convert a date to YYYY:MM:DD format.
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
    """
    Get the UTC date now in YYYY-MM-DD format.
    Returns:
        str: The date.
    """
    date = current_date()
    return date_to_ymd(date)
