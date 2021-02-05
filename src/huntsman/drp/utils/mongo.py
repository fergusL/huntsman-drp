from collections import abc
import numpy as np
from astropy import units as u

# These are responsible for converting arbitary types into something mongo can store
MONGO_ENCODINGS = {np.bool_: bool,
                   np.float64: float,
                   np.float32: float,
                   np.int32: int,
                   np.int64: int}

# These are responsible for converting string keys into equivalent mongoDB operators
MONGO_OPERATORS = {"equal": "$eq",
                   "not_equal": "$ne",
                   "greater_than": "$gt",
                   "greater_than_equal": "$gte",
                   "less_than": "$lt",
                   "less_than_equal": "$lte",
                   "in": "$in",
                   "not_in": "$nin"}


def encode_mongo_query(value):
    """ Encode object for a pymongo query.
    Args:
        value (object): The data to encode.
    Returns:
        object: The encoded data.
    """
    if isinstance(value, u.Quantity):
        return encode_mongo_query(value.value)
    if isinstance(value, abc.Mapping):
        for k, v in value.items():
            value[k] = encode_mongo_query(v)
    elif isinstance(value, str):
        pass  # Required because strings are also iterables
    elif isinstance(value, abc.Iterable):
        value = [encode_mongo_query(v) for v in value]
    else:
        for oldtype, newtype in MONGO_ENCODINGS.items():
            if isinstance(value, oldtype):
                value = newtype(value)
                break
    return value
