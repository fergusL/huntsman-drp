from collections import abc
import numpy as np

# These are responsible for converting arbitary types into something mongo can store
MONGO_ENCODINGS = {np.bool_: bool,
                   np.float64: float,
                   np.float32: float,
                   np.int32: int,
                   np.int64: int}

# These are responsible for converting string keys into equivalent mongoDB operators
MONGO_OPERATORS = {"equals": "$eq",
                   "not_equals": "$ne",
                   "greater_than": "$gt",
                   "greater_than_equals": "$gte",
                   "less_than": "$lt",
                   "less_than_equals": "$lte",
                   "in": "$in",
                   "not_in": "$nin"}


def encode_mongo_data(value):
    """ Encode object for a pymongodb query.
    Args:
        value (object): The data to encode.
    Returns:
        object: The encoded data.
    """
    if isinstance(value, abc.Mapping):
        for k, v in value.items():
            value[k] = encode_mongo_data(v)
    elif isinstance(value, str):
        pass  # Required because strings are also iterables
    elif isinstance(value, abc.Iterable):
        value = [encode_mongo_data(v) for v in value]
    else:
        for oldtype, newtype in MONGO_ENCODINGS.items():
            if isinstance(value, oldtype):
                value = newtype(value)
                break
    return value
