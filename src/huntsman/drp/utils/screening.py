from contextlib import suppress
import numpy as np

from huntsman.drp.core import get_logger

_VALID_KEYS = "maximum", "minimum", "equals", "not_equals"


def satisfies_criteria(metric_values, criteria, logger=None, metric_name="property"):
    """ Return a boolean array indicating which values satisfy the criteria.
    Args:
        metric_values (np.array): The data to test, pretaining to a specific quality metric.
        criteria (dict): The dictionary of criteria. Supported keys are `minumum`, `maximum`,
            `equals` and `not_equals`. If a non-supported key is present, a ValueError will be
            raised.
        logger (Logger, optional): The logger.
        metric_name (str, optional): The name of the metric.
    Returns: (boolean array): True if satisfies criteria, False otherise.
    """
    logger = get_logger if logger is None else logger
    metric_values = np.array(metric_values)  # Make sure data is an array

    invalid_keys = [k for k in criteria.keys() if k not in _VALID_KEYS]
    if len(invalid_keys) != 0:
        raise ValueError(f"Invalid keys found in criteria dictionary: {invalid_keys}.")

    satisfies = np.ones_like(metric_values, dtype="bool")  # True where values satisfy criteria
    with suppress(KeyError):
        value = criteria["minimum"]
        logger.debug(f"Applying lower threshold in {metric_name} of {value}.")
        satisfies = np.logical_and(satisfies, metric_values >= value)
    with suppress(KeyError):
        value = criteria["maximum"]
        logger.debug(f"Applying upper threshold in {metric_name} of {value}.")
        satisfies = np.logical_and(satisfies, metric_values < value)
    with suppress(KeyError):
        value = criteria["equals"]
        logger.debug(f"Applying equals opterator to {metric_name} with value {value}.")
        satisfies = np.logical_and(satisfies, metric_values == value)
    with suppress(KeyError):
        value = criteria["not_equals"]
        logger.debug(f"Applying not-equals opterator to {metric_name} with value {value}.")
        satisfies = np.logical_and(satisfies, metric_values != value)

    logger.debug(f"{satisfies.sum()} of {satisfies.size} values satisfy criteria"
                 f" for {metric_name}.")
    return satisfies
