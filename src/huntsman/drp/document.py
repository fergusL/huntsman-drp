""" Classes to represent dataIds. """
from collections import abc
from contextlib import suppress

from huntsman.drp.core import get_config
from huntsman.drp.utils.mongo import encode_mongo_filter


class Document(abc.Mapping):
    """ A dataId behaves like a dictionary but makes it easier to compare between dataIds.
    DataId objects are hashable, whereas dictionaries are not. This allows them to be used in sets.
    """
    _required_keys = tuple()

    def __init__(self, document, **kwargs):
        super().__init__()

        if document is None:
            document = {}

        elif isinstance(document, Document):
            document = document._document

        # Check all the required information is present
        self._validate_document(document)

        self._document = document.copy()

    # Special methods

    def __eq__(self, o):
        with suppress(KeyError):
            return all([self[k] == o[k] for k in self._required_keys])
        return False

    def __hash__(self):
        return hash(tuple([self[k] for k in self._required_keys]))

    def __getitem__(self, key):
        return self._document[key]

    def __setitem__(self, key, item):
        self._document[key] = item

    def __delitem__(self, item):
        del self._document[item]

    def __iter__(self):
        return self._document.__iter__()

    def __len__(self):
        return len(self._document)

    def __str__(self):
        return str({k: self._document[k] for k in self._required_keys})

    # Public methods

    def values(self):
        return self._document.values()

    def items(self):
        return self._document.items()

    def keys(self):
        return self._document.keys()

    def update(self, d):
        self._document.update(d)

    def to_mongo(self):
        return encode_mongo_filter(self._document)

    # Private methods

    def _validate_document(self, document):
        """
        """
        if not all([k in document for k in self._required_keys]):
            raise ValueError(f"Document does not contain all required keys: {self._required_keys}.")


class RawExposureDocument(Document):

    _required_keys = ["filename"]

    def __init__(self, document, config=None, **kwargs):

        if config is None:
            config = get_config()  # Do not store the config as we will be making many DataIds

        self._required_keys.extend(config["fits_header"]["required_columns"])

        super().__init__(document=document, **kwargs)


class CalibDocument(Document):

    _required_keys = ("calibDate", "datasetType", "filename", "ccd")

    _required_keys_type = {"flat": ("filter",)}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _validate_document(self, document):
        """
        """
        super()._validate_document(document)

        keys = self._required_keys_type.get(document["datasetType"], None)
        if not keys:
            return

        if not all([k in document for k in keys]):
            raise ValueError(f"Document does not contain all required keys: {keys}.")
