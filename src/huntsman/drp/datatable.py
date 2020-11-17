"""Code to interface with the Huntsman mongo database."""
from collections import abc
from copy import deepcopy
from functools import partial
from datetime import timedelta
from urllib.parse import quote_plus

import pandas as pd

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import current_date, parse_date
from huntsman.drp.utils.query import QueryCriteria, encode_mongo_value


def _apply_operation(func, metadata):
    """ Apply a function to the metadata. metadata can either be a mappable, in which case the
    function is called with metadata as its first argument, an iterable, in which case the function
    will be successively applied to each of its items (assumed to be mappings), or a pd.DataFrame,
    in which case the function will be applied to each of its rows.
    Args:
        func (Function): The function to apply.
        metadata (pd.DataFrame, abc.Mapping or abc.Iterable): The metadata to process.
    """
    if isinstance(metadata, pd.DataFrame):
        for _, item in metadata.iterrows():
            func(encode_mongo_value(item))
    elif isinstance(metadata, abc.Mapping):
        func(encode_mongo_value(metadata))
    elif isinstance(metadata, abc.Iterable):
        for item in metadata:
            func(encode_mongo_value(item))
    else:
        raise TypeError(f"Invalid metadata type: {type(metadata)}.")


def require_unlocked(func):
    """ Raise a PermissionError if the function is called when the table is locked. """

    def _require_unlocked(self, *args, **kwargs):
        if self.is_locked:
            raise PermissionError(f"{self} must be unlocked to call {func.__name__}. To unlock,"
                                  "call the `unlock` method.")
        return func(self, *args, **kwargs)
    return _require_unlocked


class DataTable(HuntsmanBase):
    """ The primary goal of DataTable objects is to provide a minimal, easily-configurable and
    user-friendly interface between the mongo database and the DRP that enforces standardisation
    of new documents. """
    _required_columns = None
    _unique_columns = ("filename", )  # Required to identify a unique document
    is_locked = False

    def __init__(self, **kwargs):
        HuntsmanBase.__init__(self, **kwargs)
        self._date_key = self.config["mongodb"]["date_key"]

        # Initialise the DB
        self._table_name = self.config["mongodb"]["tables"][self._table_key]
        db_name = self.config["mongodb"]["db_name"]
        self._initialise(db_name, self._table_name)

    def lock(self):
        self.is_locked = True

    def unlock(self):
        self.is_locked = False

    def query(self, criteria=None, date_start=None, date_end=None, date=None):
        """ Get data for one or more matches in the table.
        Args:
            criteria (dict, optional): The query criteria.
            date_start (object, optional): The start of the queried date range.
            date_end (object, optional):  The end of the queried date range.
            date (object, optional): The exact date to query on.
        Returns:
            pd.DataFrame: The query result.
        """
        if criteria is None:
            criteria = {}

        # Add date range to criteria if given
        date_criteria = {}
        if date_start is not None:
            date_criteria.update({"greater_than_equals": parse_date(date_start)})
        if date_end is not None:
            date_criteria.update({"less_than": parse_date(date_end)})
        if date is not None:
            date_criteria.update({"equals": parse_date(date)})
        if date_criteria:
            criteria = deepcopy(criteria)
            criteria[self._date_key] = date_criteria

        # Perform the query
        self.logger.debug(f"Performing query with criteria: {criteria}.")
        criteria = QueryCriteria(criteria).to_mongo()
        cursor = self._table.find(criteria)

        # Convert to a DataFrame object
        df = pd.DataFrame(list(cursor))
        self.logger.debug(f"Query returned {df.shape[0]} results.")

        return df

    def query_latest(self, days=0, hours=0, seconds=0, criteria=None):
        """ Convenience function to query the latest files in the db.
        Args:
            days (int): default 0.
            hours (int): default 0.
            seconds (int): default 0.
            criteria (dict, optional): Criteria for the query.
        Returns:
            list: Query result.
        """
        date_now = current_date()
        date_start = date_now - timedelta(days=days, hours=hours, seconds=seconds)
        return self.query(date_start=date_start, criteria=criteria)

    @require_unlocked
    def insert(self, metadata, overwrite=False):
        """ Insert a new document into the table after ensuring it is valid and unique.
        Args:
            data_id (dict): The dictionary specifying the single document to delete.
            overwrite (bool): If True, will overwrite the existing document for this dataId.
        """
        fn = partial(self._insert_one, overwrite=overwrite)
        return _apply_operation(fn, metadata)

    @require_unlocked
    def update(self, metadata, upsert=False):
        """ Update a single document in the table.
        Args:
            data_id (dict): The data ID of the document to update.
            metadata (dict): The new metadata to be inserted.
            upsert (bool): If True, will create a new document if there is no matching entry.
        """
        fn = partial(self._update_one, upsert=upsert)
        return _apply_operation(fn, metadata)

    @require_unlocked
    def delete(self, metadata):
        """ Delete one document from the table.
        Args:
            data_id (dict): The dictionary specifying the single document to delete.
        """
        return _apply_operation(self._delete_one, metadata)

    def _initialise(self, db_name, table_name):
        """ Initialise the database.
        Args:
            db_name (str): The name of the (mongo) database.
            table_name (str): The name of the table (mongo collection).
        """
        # Connect to the mongodb
        hostname = self.config["mongodb"]["hostname"]
        port = self.config["mongodb"]["port"]
        if "username" in self.config["mongodb"].keys():
            username = quote_plus(self.config["mongodb"]["username"])
            password = quote_plus(self.config["mongodb"]["password"])
            uri = f"mongodb://{username}:{password}@{hostname}/{db_name}?ssl=true"
            self._client = MongoClient(uri)
        else:
            self._client = MongoClient(hostname, port)
        try:
            self._client.server_info()
            self.logger.debug(f"Connected to mongodb at {hostname}:{port}.")
        except ServerSelectionTimeoutError as err:
            self.logger.error(f"Unable to connect to mongodb at {hostname}:{port}.")
            raise err
        self._db = self._client[db_name]
        self._table = self._db[table_name]

    def _insert_one(self, metadata, overwrite):
        """ Insert a new document into the table after ensuring it is valid and unique.
        Args:
            data_id (dict): The dictionary specifying the single document to delete.
            overwrite (bool): If True, will overwrite the existing document for this dataId.
        """
        # Ensure required columns exist
        if self._required_columns is not None:
            for column_name in self._required_columns:
                if column_name not in metadata.keys():
                    raise ValueError(f"New document missing required column: {column_name}.")

        # Check for matches in data table
        unique_id = self._get_unique_id(metadata)
        query_result = self.query(criteria=self._get_unique_id(metadata))
        query_count = query_result.shape[0]

        if query_count == 1:
            if overwrite:
                self.delete(query_result)
            else:
                raise ValueError(f"Found existing document for {unique_id} in {self}."
                                 " Pass overwrite=True to overwrite.")
        elif query_count != 0:
            raise ValueError(f"Multiple matches found for document in {self}: {unique_id}.")

        # Insert the new document
        self.logger.debug(f"Inserting new document into {self}: {metadata}.")
        self._table.insert_one(metadata)

    def _update_one(self, metadata, upsert):
        """ Update a single document in the table. MongoDB edits the first matching
        document, so we need to check we are only matching with a single document. A new document
        will be created if there are no matches in the table.
        Args:
            data_id (dict): The data ID of the document to update.
            metadata (dict): The new metadata to be inserted.
            upsert (bool): If True, will create a new document if there is no matching entry.
        """
        data_id = self._get_unique_id(metadata)
        query_count = self.query(criteria=data_id).shape[0]
        if query_count > 1:
            raise RuntimeError(f"data ID matches with more than one document: {data_id}.")
        elif query_count == 0:
            if upsert:
                new_metadata = data_id.copy().update(metadata.copy())
                return self._insert_one(new_metadata)
            else:
                raise RuntimeError(f"No matching entry for {data_id} in {self}.")
        else:
            self._table.update_one(data_id, {'$set': metadata})

    def _delete_one(self, data_id):
        """ Delete one document from the table. MongoDB deletes the first matching document,
        so we need to check we are only matching with a single document. A warining is logged if
        no document is matched.
        Args:
            data_id (dict): The dictionary specifying the single document to delete.
        """
        query_count = self.query(criteria=data_id).shape[0]
        if query_count > 1:
            raise RuntimeError(f"Metadata matches with more than one document: {data_id}.")
        elif query_count == 0:
            self.logger.warning(f"Tried to delete non-existent document from {self}:"
                                f" {data_id}.")
        elif query_count == 1:
            self.logger.debug(f"Deleting {data_id} from {self}.")
            self._table.delete_one(data_id)

    def _get_unique_id(self, metadata):
        """ Return the unique identifier for the metadata.
        Args:
            metadata (abc.Mapping): The metadata.
        Returns:
            dict: The unique document identifier.
        """
        return encode_mongo_value({k: metadata[k] for k in self._unique_columns})


class RawDataTable(DataTable):
    """Table to store metadata for raw data synced via NiFi from Huntsman."""
    _table_key = "raw_data"
    is_locked = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._required_columns = self.config["fits_header"]["required_columns"]


class RawQualityTable(DataTable):
    """ Table to store data quality metadata for raw data. """
    _table_key = "raw_quality"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class MasterCalibTable(DataTable):
    """ Table to store metadata for master calibs. """
    _table_key = "master_calib"
    _required_columns = ("filename", "calibDate")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
