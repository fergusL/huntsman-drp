"""Generic functions for huntsman-drp."""
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
    if type(object) is datetime:
        return object
    return parse_date_dateutil(object)
