from collections import abc
from contextlib import suppress
from copy import deepcopy
import numpy as np

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.mongo import MONGO_OPERATORS, encode_mongo_data


# These are responsible for applying logical operators based on a string key
OPERATORS = {"equals": lambda x, y: x == y,
             "not_equals": lambda x, y: x != y,
             "greater_than": lambda x, y: x > y,
             "greater_than_equals": lambda x, y: x >= y,
             "less_than": lambda x, y: x < y,
             "less_than_equals": lambda x, y: x <= y,
             "in": lambda x, y: np.isin(x, y),
             "not_in": lambda x, y: np.isin(x, y, invert=True)}


def encode_mongo_value(value):
    """ Encode object for a pymongodb query.
    Args:
        value (object): The value to encode.
    Returns:
        object: The encoded value.
    """
    with suppress(AttributeError):
        value = value.to_dict()
    if isinstance(value, abc.Mapping):
        for k, v in value.items():
            value[k] = encode_mongo_value(v)
    elif isinstance(value, str):
        pass
    elif isinstance(value, abc.Iterable):
        value = [encode_mongo_value(v) for v in value]
    elif isinstance(value, np.bool_):
        value = bool(value)
    elif isinstance(value, np.int32):
        value = int(value)
    elif isinstance(value, np.float32):
        value = float(value)
    elif isinstance(value, np.int64):
        value = int(value)
    elif isinstance(value, np.float64):
        value = float(value)
    return value


class Criteria(HuntsmanBase):
    """ Criteria objects generally correspond to single metadata columns."""

    _mongo_operators = MONGO_OPERATORS
    _operator_keys = list(OPERATORS.keys())

    def __init__(self, criteria):
        self.criteria = self._parse_criteria(criteria)

    def is_satisfied(self, values):
        """ Return a boolean array indicating which values satisfy the criteria.
        Args:
            values (np.array): The test values.
            criteria (abc.Mapping): The criteria dictionary.
        Returns:
            boolean array: True if satisfies criteria, False otherise.
        """
        satisfied = np.ones_like(values, dtype="bool")
        for operator, opvalue in self.criteria.items():
            satisfied = np.logical_and(satisfied, OPERATORS[operator](values, opvalue))

        return satisfied

    def to_mongo(self):
        """ Return the criteria as a dictionary suitable for pymongo.
        Returns:
            dict: The query dictionary.
        """
        new = {}
        for k, v in self.criteria.items():
            try:
                k = self._mongo_operators[k]
            except KeyError:
                if k not in self._mongo_operators.values():
                    raise KeyError(f"Unrecognised criteria operator: {k}. Should be one of: "
                                   f" {list(self._mongo_operators.keys())}")
            if v is not None:
                new[k] = encode_mongo_data(v)
        return new

    def _parse_criteria(self, criteria):
        """ Parse the criteria into a standardised format.
        Args:
            query_criteria (abc.Mappable): The query criteria to be parsed.
        Returns:
            abc.Mappable: The parsed criteria.
        """
        if isinstance(criteria, QueryCriteria):
            return deepcopy(criteria.criteria)

        # If a direct mapping, assume 'equals' or 'in' operator
        if not isinstance(criteria, abc.Mapping):
            if isinstance(criteria, abc.Iterable) and not isinstance(criteria, str):
                criteria = {"in": criteria}
            else:
                criteria = {"equals": criteria}

        # Check the operator keys are valid
        for key in criteria.keys():
            if key not in self._operator_keys:
                raise ValueError(f"Unrecognised operator in query criteria: '{key}''."
                                 f" Valid operator names are: {self._operator_keys}.")
        return deepcopy(criteria)


class QueryCriteria(HuntsmanBase):
    """ The purpose of this class is to provide an abstract implementation of a multi-column
    query criteria, allowing configured criteria to be easily converted to whatever format the
    database requires and be applied to DataFrames.
    """

    def __init__(self, criteria, *args, **kwargs):
        """
        Args:
            criteria (abc.Mappable): The query criteria.
            *args, **kwargs: Parsed to HuntsmanBase.
        """
        super().__init__(*args, **kwargs)
        # Create criteria objects
        self.criteria = {}
        for column_name, column_criteria in criteria.items():
            self.criteria[column_name] = Criteria(column_criteria)

    def to_mongo(self):
        """ Return the criteria as a dictionary suitable for pymongo.
        Returns:
            dict: The query dictionary.
        """
        result = {}
        for column_name, column_criteria in self.criteria.items():
            result[column_name] = column_criteria.to_mongo()
        return result

    def is_satisfied(self, df):
        """ Return a boolean array indicating which rows satisfy the criteria.
        Args:
            df (pd.DataFrame): The DataFrame to test.
        Returns:
            boolean array: True if satisfies criteria, False otherise.
        """
        row_is_satisfied = np.ones(df.shape[0], dtype="bool")

        for column_name, column_criteria in self.criteria.items():
            column_is_satisfied = column_criteria.is_satisfied(df[column_name].values)
            row_is_satisfied = np.logical_and(row_is_satisfied, column_is_satisfied)

        return row_is_satisfied
