import numpy as np


def encode_metadata(dictionary):
    """Correct the encoding of python dictionaries so they can be encoded to mongodb.
    Args:
        dictionary : dictionary instance to add as document.
    Returns:
        dict : New dictionary with corrected encodings
    """
    new = {}
    for key1, val1 in dictionary.items():
        # Handle nested dictionaries
        if isinstance(val1, dict):
            val1 = encode_metadata(val1)
        # Do type conversions
        elif isinstance(val1, np.bool_):
            val1 = bool(val1)
        elif isinstance(val1, np.int64):
            val1 = int(val1)
        elif isinstance(val1, np.float64):
            val1 = float(val1)
        new[key1] = val1
    return new
