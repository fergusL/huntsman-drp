""" Classes to represent dataIds. """
from contextlib import suppress

from huntsman.drp.core import get_config


class DataId(object):
    """ A dataId behaves like a dictionary but makes it easier to compare between dataIds.
    DataId objects are hashable, whereas dictionaries are not. This allows them to be used in sets.
    """
    _required_keys = None

    def __init__(self, document, **kwargs):

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

    def __str__(self):
        return str({k: self._document[k] for k in self._required_keys})

    # Public methods

    def values(self):
        return self._document.values()

    def items(self):
        return self._document.items()

    def keys(self):
        return self.document.keys()

    def to_dict(self):
        return self._document.copy()

    # Private methods

    def _validate_document(self, document):
        """
        """
        if not all([k in document for k in self._required_keys]):
            raise ValueError(f"Document does not contain all required keys: {self._required_keys}.")


class RawExposureId(DataId):

    _required_keys = ["filename"]

    def __init__(self, document, config=None, **kwargs):

        if config is None:
            config = get_config()  # Do not store the config as we will be making many DataIds

        self._required_keys.extend(config["fits_header"]["required_columns"])

        super().__init__(document=document, **kwargs)


class CalibId(DataId):

    _required_keys = ("calibDate", "datasetType", "filename")

    _required_keys_type = {"bias": ("calibDate", "ccd"),
                           "dark": ("calibDate", "ccd"),
                           "flat": ("calibDate", "ccd", "filter")}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _validate_document(self, document):
        """
        """
        super()._validate_document(document)

        keys = self._required_keys_type[document["datasetType"]]

        if not all([k in document for k in keys]):
            raise ValueError(f"Document does not contain all required keys: {keys}.")
