""" Classes to represent dataIds. """
from collections import abc
from contextlib import suppress

from astropy.coordinates import SkyCoord
from astropy import units as u

from huntsman.drp.core import get_config
from huntsman.drp.utils.date import parse_date
from huntsman.drp.utils.mongo import encode_mongo_filter, unflatten_dict


class Document(abc.Mapping):
    """ A dataId behaves like a dictionary but makes it easier to compare between dataIds.
    DataId objects are hashable, whereas dictionaries are not. This allows them to be used in sets.
    """
    _required_keys = set()

    def __init__(self, document, validate=True, copy=False, **kwargs):
        super().__init__()

        if document is None:
            document = {}

        elif isinstance(document, Document):
            document = document._document

        if copy:
            document = document.copy()

        # Check all the required information is present
        if validate and self._required_keys:
            self._validate_document(document)

        self._document = document

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
        if self._required_keys:
            return str({k: self._document[k] for k in self._required_keys})
        return str(self._document)

    def __repr__(self):
        if self._required_keys:
            return repr({k: self._document[k] for k in self._required_keys})
        return repr(self._document)

    # Public methods

    def values(self):
        return self._document.values()

    def items(self):
        return self._document.items()

    def keys(self):
        return self._document.keys()

    def update(self, d):
        self._document.update(d)

    def to_mongo(self, flatten=True):
        """ Get the full mongo filter for the document """
        d = encode_mongo_filter(self._document)
        if not flatten:
            return unflatten_dict(d)
        return d

    def get_mongo_id(self):
        """ Get the unique mongo ID for the document """
        doc = {k: self[k] for k in self._required_keys}
        return encode_mongo_filter(doc)

    # Private methods

    def _validate_document(self, document):
        """
        """
        if not all([k in document for k in self._required_keys]):
            missing_keys = [k for k in self._required_keys if k not in document.keys()]
            raise ValueError(f"Document missing required keys: {missing_keys}.")


class RawExposureDocument(Document):

    _required_keys = set(["filename"])

    def __init__(self, document, config=None, **kwargs):

        if config is None:
            config = get_config()  # Do not store the config as we will be making many DataIds

        self._required_keys.update(config["fits_header"]["required_columns"])

        super().__init__(document=document, **kwargs)

        if "date" not in self.keys():
            self["date"] = parse_date(self["dateObs"])

    def get_central_skycoord(self):
        """ Return the central celestial coordinate of the exposure using the WCS info.
        Returns:
            astropy.coordinates.SkyCoord: The central coordinate.
        """
        ra = self["metrics"]["ra_centre"] * u.deg
        dec = self["metrics"]["dec_centre"] * u.deg
        return SkyCoord(ra=ra, dec=dec)


class CalibDocument(Document):

    _required_keys = set(["calibDate", "datasetType", "filename", "ccd"])

    _required_keys_type = {"flat": ("filter",)}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if "date" not in self.keys():
            self["date"] = parse_date(self["calibDate"])

    def _validate_document(self, document):
        """
        """
        super()._validate_document(document)

        keys = self._required_keys_type.get(document["datasetType"], None)
        if not keys:
            return

        if not all([k in document for k in keys]):
            raise ValueError(f"Document does not contain all required keys: {keys}.")
