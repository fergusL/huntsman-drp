from contextlib import suppress
from collections import abc, MutableMapping
from copy import deepcopy
from collections import defaultdict

from huntsman.drp.utils.mongo import MONGO_OPERATORS, encode_mongo_query


def flatten_dict(d, parent_key=None, sep='.'):
    """ Flatten a nested dictionary, for example to dot notation.

    Parameters
    ----------
    d : dict
        The dictionary to flatten.
    parent_key : str, optional
        will be prepended to keys of flattened dict, by default None.
    sep : str, optional
        Separater character between parent_key and key name,
        by default '.' as required for pymongo.

    Returns
    -------
    dict
        The flattened dictionary.
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


class Query():
    """
    """

    def __init__(self, document=None, constraints=None):
        """Query class for building a pymongo query.

        Parameters
        ----------
        document : dict, optional
            A dictionary containing key, value pairs to be matched against other documents,
            by default None.
        constraints : dict, optional
            A dictionary containing other search criteria which can include the mongo operators
            defined in the `huntsman.drp.utils.mongo.MONGO_OPERATORS`, by default None.

        https://docs.mongodb.com/manual/reference/operator/query/
        """
        if document is None:
            document = {}

        if constraints is None:
            constraints = {}

        with suppress(AttributeError):
            document = document.to_dict()

        with suppress(AttributeError):
            constraints = constraints.to_dict()

        if not isinstance(document, abc.Mapping):
            raise TypeError(f"document should be a mapping, got {type(document)}.")

        if not isinstance(constraints, abc.Mapping):
            raise TypeError(f"constraints should be a mapping, got {type(constraints)}.")

        # Store as flattened dictionaries using dot format recongnised by pymongo
        self.document = flatten_dict(deepcopy(document))
        self.constraints = flatten_dict(deepcopy(constraints))

    def to_mongo(self):
        """ Builds pymongo query from inputs, transforming any constraints that utilise
        mongo operators into the appropriate form.

        Returns
        -------
        defaultdict
            The properly formatted pymongo query dict.
        """
        mongo_query = defaultdict(dict)

        # Map constraint operators to their mongodb forms
        for k, constraint in self.constraints.items():

            # Extract the key, operator pair from the flattened key
            k = k.split(".")
            operator = MONGO_OPERATORS[k[-1]]
            key = ".".join(k[:-1])

            # Add the constraint to the constraint dict for this key
            mongo_query[key][operator] = encode_mongo_query(constraint)

        # Update the mongo query with the original document
        for k, value in self.document.items():
            value = encode_mongo_query(value)
            if k not in mongo_query.keys():
                mongo_query[k] = value
            else:
                mongo_query[k].update({MONGO_OPERATORS["equal"]: value})

        return mongo_query
