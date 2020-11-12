"""Code to interface with the Huntsman database."""
from contextlib import suppress
from datetime import timedelta
from urllib.parse import quote_plus

import numpy as np
import pandas as pd

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from huntsman.drp.utils.date import parse_date, current_date
from huntsman.drp.utils.query import Criteria, QueryCriteria, encode_mongo_value
from huntsman.drp.base import HuntsmanBase


def new_document_validation(func):
    """Wrapper to validate a new document."""

    def wrapper(self, metadata, *args, **kwargs):
        self._validate_new_document(metadata)
        return func(self, metadata, *args, **kwargs)
    return wrapper


def edit_permission_validation(func):
    """Wrapper to check permission to edit DB entries."""

    def wrapper(self, *args, **kwargs):
        self._validate_edit_permission(**kwargs)
        return func(self, *args, **kwargs)
    return wrapper


class DataTable(HuntsmanBase):
    """ """
    _required_columns = None
    _allow_edits = True

    def __init__(self, **kwargs):
        HuntsmanBase.__init__(self, **kwargs)
        self._date_key = self.config["mongodb"]["date_key"]

        # Initialise the DB
        self._table_name = self.config["mongodb"]["tables"][self._table_key]
        db_name = self.config["mongodb"]["db_name"]
        self._initialise(db_name, self._table_name)

    def _initialise(self, db_name, table_name):
        """
        Initialise the datebase.
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

    def find(self, criteria, expected_count=None):
        """
        Find metadata for one or more matches in a table.
        Args:
            data_id (dict): The data ID to search for.
            expected_count (int, optional): The expected number of matches. If given and it does
                not match the actual number of matches, a `RuntimeError` is raised.
        Returns:
            list of dict: The find result.
        """
        if criteria is not None:
            criteria = QueryCriteria(criteria).to_mongo()

        cursor = self._table.find(criteria)
        df = pd.DataFrame(list(cursor))

        if expected_count is not None:
            if df.shape[0] != expected_count:
                raise RuntimeError(f"Expected {expected_count} matches but found {df.shape[0]}.")
        return df

    def query(self, date=None, date_start=None, date_end=None, criteria=None):
        """
        Query the table, optionally with a date range.
        Args:
            date (date, optional): The specific date to query on.
            date_start (date, optional): The earliest date of returned rows.
            date_end (date, optional): The latest date of returned rows.
            query_dict (abc.Mapping, optional): Parsed to the query.
        Returns:
            list of dict: Dictionary of query results.
        """
        df = self.find(criteria=criteria)

        # Apply date selection using parse_date
        # TODO remove this in favour of pymongo date handling
        if self._date_key in df.columns:

            # Build query criteria
            date_criteria = {}
            if date is not None:
                date_criteria["equals"] = parse_date(date)
            if date_start is not None:
                date_criteria["greater_than_equals"] = parse_date(date_start)
            if date_end is not None:
                date_criteria["less_than"] = parse_date(date_end)

            # Apply to dataframe
            parsed_dates = np.array([parse_date(d) for d in df[self._date_key].values])
            keep = Criteria(date_criteria).is_satisfied(parsed_dates)
            df = df[keep].reset_index(drop=True)

        self.logger.debug(f"Query returned {df.shape[0]} results.")
        return df

    def query_latest(self, days=0, hours=0, seconds=0, criteria=None):
        """
        Convenience function to query the latest files in the db.
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

    def query_matches(self, values, match_key="filename", one_to_one=True, **kwargs):
        """ Get rows matching values of a certain key.
        Args:
            table (huntsman.drp.datatable.DataTable): The data table to match with.
            match_key (str, optional): The key to match on. Default: 'filename'.
            one_to_one (bool): If True (default), require one-to-one matching.
            **kwargs: Parsed to table.query
        Returns:
            pd.DataFrame: The matched query result.
        """
        df_query = self.query(**kwargs)

        # Use the matching key as the DataFrame index
        df_query.set_index(match_key, inplace=True)

        # Return the matched DataFrame.
        df_matched = pd.DataFrame([df_query.loc[v] for v in values])
        df_matched[match_key] = values

        if one_to_one:
            if df_matched.shape[0] != len(values):
                raise RuntimeError("One-to-one criteria not satisfied for matching query.")

        return df_matched

    @edit_permission_validation
    @new_document_validation
    def insert_one(self, metadata, **kwargs):
        """
        Insert a single entry into the table.
        Args:
            metadata (dict): The document to insert.
        """
        del_id_key = "_id" not in metadata.keys()  # pymongo adds _id to metadata automatically
        self._table.insert_one(metadata)
        if del_id_key:
            del metadata["_id"]

    def insert_many(self, metadata_list, **kwargs):
        """
        Insert a single entry into the table.
        Args:
            metadata_list (list of dict): The documents to insert.
            **kwargs: Parsed to `insert_one`.
        """
        for metadata in metadata_list:
            self.insert_one(metadata, **kwargs)

    @edit_permission_validation
    def update_document(self, data_id, metadata, **kwargs):
        """
        Update the document associated with the data_id.
        Args:
            data_id (dict): Dictionary of key: value pairs identifying the document.
            data (dict): Dictionary of key: value pairs to update in the database. The field will
                be created if it does not already exist.
        Returns:
            `pymongo.results.UpdateResult`: The result of the update operation.
        """
        self.find(data_id, expected_count=1)  # Make sure there is only one match

        # Since we are using pymongo we will have to do some parsing
        metadata = encode_mongo_value(metadata)

        result = self._table.update_one(data_id, {'$set': metadata}, upsert=False)
        if result.matched_count != 1:
            raise RuntimeError(f"Unexpected number of documents updated: {result.deleted_count}.")

        return result

    @edit_permission_validation
    def delete_document(self, data_id, **kwargs):
        """
        Delete the document associated with the data_id.
        Args:
            data_id (dict): Dictionary of key: value pairs identifying the document.
        Returns:
            `pymongo.results.UpdateResult`: The result of the delete operation.
        """
        with suppress(AttributeError):
            data_id = data_id.to_dict()
        if data_id is not None:
            data_id = encode_mongo_value(data_id)

        self.find(data_id, expected_count=1)  # Make sure there is only one match
        result = self._table.delete_one(data_id)
        if result.deleted_count != 1:
            raise RuntimeError(f"Unexpected number of documents deleted: {result.deleted_count}.")

        return result

    def update_file_data(self, filename, data, **kwargs):
        """
        Update the metadata associated with a file in the database.
        Args:
            filename (str): Modify the metadata for this file.
            data (dict): Dictionary of key: value pairs to update in the database. The field will
                be created if it does not already exist.
        Returns:
            `pymongo.results.UpdateResult`: The result of the update operation.
        """
        data_id = {'filename': filename}
        return self.update_document(data_id, data, **kwargs)

    def delete_file_data(self, filename, **kwargs):
        """
        Delete the metadata associated with a file in the database.
        Args:
            filename (str): Modify the metadata for this file.
            data (dict): Dictionary of key: value pairs to update in the database. The field will
                be created if it does not already exist.
        Returns:
            `pymongo.results.UpdateResult`: The result of the delete operation.
        """
        data_id = {'filename': filename}
        return self.delete_document(data_id, **kwargs)

    def _validate_edit_permission(self, bypass_allow_edits=False, **kwargs):
        """Raise a PermissionError if not `bypass_allow_edits` or `self._allow_edits`."""
        if not (bypass_allow_edits or self._allow_edits):
            raise PermissionError("Edits are not allowed by-default for this table. If you are"
                                  "sure you want to do this, use `bypass_allow_edits=True`.")

    def _validate_new_document(self, metadata):
        """Make sure the required columns are in the metadata."""
        if self._required_columns is None:
            return
        missing = [k for k in self._required_columns if k not in metadata.keys()]
        if len(missing) != 0:
            raise ValueError(f"Missing columns for update: {missing}.")
        self.find(metadata, expected_count=0)


class RawDataTable(DataTable):
    """Table to store metadata for raw data synced via NiFi from Huntsman."""
    _table_key = "raw_data"
    _date_key = "taiObs"
    _allow_edits = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._required_columns = self.config["fits_header"]["required_columns"]


class RawQualityTable(DataTable):
    """ Table to store data quality metadata for raw data. """
    _table_key = "raw_quality"
    _required_columns = ("filename",)
    _allow_edits = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class MasterCalibTable(DataTable):
    """ Table to store metadata for master calibs. """
    _table_key = "master_calib"
    _required_columns = ("filename", "calibDate")
    _allow_edits = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
