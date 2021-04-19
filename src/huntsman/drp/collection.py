"""Code to interface with the Huntsman mongo database."""
from contextlib import suppress
from datetime import timedelta
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import current_date, parse_date
from huntsman.drp.document import Document, RawExposureDocument, CalibDocument
from huntsman.drp.utils.mongo import encode_mongo_filter, mongo_logical_or, mongo_logical_and
from huntsman.drp.utils.screening import SCREEN_SUCCESS_FLAG


class Collection(HuntsmanBase):
    """ This class is used to interface with the mongodb. It is responsible for performing queries
    and inserting/updating/deleting documents, as well as validating new documents.
    """
    _unique_columns = "filename",  # Required to identify a unique document

    def __init__(self, table_name, **kwargs):
        super().__init__(**kwargs)

        self._date_key = self.config["mongodb"]["date_key"]
        self._table_name = table_name

        # Initialise the DB
        db_name = self.config["mongodb"]["db_name"]
        self._connect(db_name, self._table_name)

    # Public methods

    def count_documents(self, document_filter=None):
        """ Count the number of documents (matching document_filter criteria) in table.
        Args:
            document_filter (dict, optional): A dictionary containing key, value pairs to be matched
            against other documents, by default None
        Returns:
            int: The number of matching documents in the table.
        """
        if document_filter is None:
            document_filter = {}
        return self._table.count_documents(document_filter)

    def find(self, document_filter=None, date_start=None, date_end=None, date=None, key=None,
             screen=False, quality_filter=False):
        """Get data for one or more matches in the table.
        Args:
            document_filter (dict, optional): A dictionary containing key, value pairs to be
                matched against other documents, by default None
            date_start (object, optional): Constrain query to a timeframe starting at date_start,
                by default None.
            date_end (object, optional): Constrain query to a timeframe ending at date_end, by
                default None.
            date (object, optional):
                Constrain query to specific date, by default None.
            key (str, optional):
                Specify a specific key to be returned from the query (e.g. filename), by default
                None.
            screen (bool, optional): If True, only return documents that passed screening.
                Default False.
            quality_filter (bool, optional): If True, only return documents that satisfy quality
                cuts. Default False.
        Returns:
            result (list): List of DataIds or key values if key is specified.
        """
        document_filter = Document(document_filter)
        with suppress(KeyError):
            del document_filter["date_modified"]  # This might change so don't match with it

        # Add date range to criteria if provided
        date_constraint = {}

        if date_start is not None:
            date_constraint.update({"greater_than_equal": parse_date(date_start)})
        if date_end is not None:
            date_constraint.update({"less_than": parse_date(date_end)})
        if date is not None:
            date_constraint.update({"equal": parse_date(date)})

        document_filter.update({self._date_key: date_constraint})

        # Screen the results if necessary
        if screen:
            document_filter[SCREEN_SUCCESS_FLAG] = True

        mongo_filter = document_filter.to_mongo()

        # Apply quality cuts
        if quality_filter:
            mongo_quality_filter = self._get_quality_filter()
            if mongo_quality_filter:
                mongo_filter = mongo_logical_and([mongo_filter, mongo_quality_filter])

        self.logger.debug(f"Performing mongo find operation with filter: {mongo_filter}.")

        documents = list(self._table.find(mongo_filter, {"_id": False}))
        self.logger.debug(f"Find operation returned {len(documents)} results.")

        if key is not None:
            return [d[key] for d in documents]

        # Skip validation to speed up - inserted documents should already be valid
        return [self._data_id_type(d, validate=False, config=self.config) for d in documents]

    def find_one(self, *args, **kwargs):
        """ Find a single matching document. If multiple matches, raise a RuntimeError.
        Args:
            *args, **kwargs: Parsed to self.find.
        Returns:
            Document or None: If there is a match return the document, else None.
        """
        documents = self.find(*args, **kwargs)
        if not documents:
            return None
        if len(documents) > 1:
            raise RuntimeError("Matched with more than one document.")
        return documents[0]

    def insert_one(self, document, overwrite=False):
        """Insert a new document into the table after ensuring it is valid and unique.
        Args:
            document (dict): The document to be inserted into the table.
            overwrite (bool, optional): If True override any existing document, by default False.
        """
        # Check the required columns exist in the new document
        document = self._data_id_type(document)
        document_id = document.get_mongo_id()

        # Check there is at most one match in the table
        if self.find_one(document_filter=document_id) is not None:
            if overwrite:
                self.update_one(document_id, to_update=document)
                return
            else:
                raise RuntimeError(f"Document {document} already exists in {self}."
                                   " Pass overwrite=True to overwrite.")

        # Prepare document to insert
        document["date_created"] = current_date()
        document["date_modified"] = current_date()
        mongo_doc = document.to_mongo()

        # Insert the document
        self.logger.debug(f"Inserting new document into {self}: {document}.")
        self._table.insert_one(mongo_doc)

    def update_one(self, document_filter, to_update, upsert=False):
        """ Update a single document in the table.
        See: https://docs.mongodb.com/manual/reference/operator/update/set/#up._S_set
        Args:
            document_filter (dict): A dictionary containing key, value pairs used to identify
                the document to update, by default None.
            to_update (dict): The key, value pairs to update within the matched document.
            upsert (bool, optional): If True perform the insert even if no matching documents
                are found, by default False.
        """
        document_filter = Document(document_filter)
        with suppress(KeyError):
            del document_filter["date_modified"]  # This might change so don't match with it

        to_update = Document(to_update)
        to_update["date_modified"] = current_date()

        mongo_filter = document_filter.to_mongo()
        mongo_update = to_update.to_mongo()

        count = self._table.count_documents(mongo_filter)
        if count > 1:
            raise RuntimeError(f"Multiple matches found for document in {self}: {document_filter}.")

        elif (count == 0) and not upsert:
            raise RuntimeError(f"No matches found for document {document_filter} in {self}. Use"
                               " upsert=True to upsert.")

        self._table.update_one(document_filter, {'$set': mongo_update}, upsert=upsert)

    def delete_one(self, document_filter):
        """Delete one document from the table.
        Args:
            document_filter (dict, optional): A dictionary containing key, value pairs used to
                identify the document to delete, by default None
        """
        document_filter = Document(document_filter, validate=False)
        mongo_filter = document_filter.to_mongo()

        count = self._table.count_documents(mongo_filter)
        if count > 1:
            raise RuntimeError(f"Multiple matches found for document in {self}: {document_filter}.")
        elif (count == 0):
            raise RuntimeError(f"No matches found for document in {self}: {document_filter}.")

        self._table.delete_one(mongo_filter)

    def insert_many(self, documents, **kwargs):
        """Insert a new document into the table.
        Args:
            documents (list): List of dictionaries that specify documents to be inserted in the
                table.
        """
        for d in documents:
            self.insert_one(d, **kwargs)

    def delete_many(self, documents, **kwargs):
        """ Delete one document from the table.
        Args:
            documents (list): List of dictionaries that specify documents to be deleted from the
                table.
        """
        for d in documents:
            self.delete_one(d, **kwargs)

    def find_latest(self, days=0, hours=0, seconds=0, **kwargs):
        """ Convenience function to query the latest files in the db.
        Args:
            days (int): default 0.
            hours (int): default 0.
            seconds (int): default 0.
        Returns:
            list: Query result.
        """
        date_now = current_date()
        date_start = date_now - timedelta(days=days, hours=hours, seconds=seconds)
        return self.find(date_start=date_start, **kwargs)

    # Private methods

    def _connect(self, db_name, table_name):
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
            self.logger.info(f"{self} connected to mongodb at {hostname}:{port}.")
        except ServerSelectionTimeoutError as err:
            self.logger.error(f"Unable to connect {self} to mongodb at {hostname}:{port}.")
            raise err
        self._db = self._client[db_name]
        self._table = self._db[table_name]

    def _get_quality_filter(self):
        """ Return the Query object corresponding to quality cuts. """
        raise NotImplementedError


class RawExposureCollection(Collection):
    """ Table to store metadata for Huntsman exposures. """

    _data_id_type = RawExposureDocument

    def __init__(self, table_name="raw_data", **kwargs):
        super().__init__(table_name=table_name, **kwargs)

    # Public methods

    def get_metrics(self, *args, **kwargs):
        """
        """
        documents = self.find_latest(*args, **kwargs)

        # Convert to a DataFrame object
        df = pd.DataFrame([d["metrics"] for d in documents])
        df.replace("", np.nan, inplace=True)

        # Add columns required to identify files
        for key in self._unique_columns:
            df[key] = [d[key] for d in documents]

        return df

    # Private methods

    def _get_quality_filter(self):
        """ Return the Query object corresponding to quality cuts.
        Returns:
            huntsman.drp.utils.query.Query: The Query object.
        """
        quality_config = self.config["quality"]["raw"].copy()

        filters = []
        for data_type, document_filter in quality_config.items():

            if document_filter is not None:
                # Create a new document filter for this data type
                document_filter["dataType"] = data_type
                filters.append(encode_mongo_filter(document_filter))

        # Allow data types that do not have any quality requirements in config
        data_types = list(quality_config.keys())
        filters.append({"dataType": {"$nin": data_types}})

        return mongo_logical_or(filters)


class MasterCalibCollection(Collection):
    """ Table to store metadata for master calibs. """

    _data_id_type = CalibDocument

    def __init__(self, table_name="master_calib", **kwargs):
        super().__init__(table_name=table_name, **kwargs)

        self._calib_types = self.config["calibs"]["types"]
        self._matching_keys = self.config["calibs"]["matching_columns"]

        # Calib validity TODO: datasetType dependence?
        self._validity = timedelta(days=self.config["calibs"]["validity"])

    def get_matching_calibs(self, data_id, calib_date):
        """ Return matching set of calib IDs for a given data_id and calib_date.
        Args:
            data_id (object): An object that can be interpreted as a data ID.
            calib_date (object): An object that can be interpreted as a date.
        Returns:
            dict: A dict of datasetType: filename.
        Raises:
            FileNotFoundError: If there is no matching calib.
        """
        calib_date = parse_date(calib_date)

        result = {}
        for calib_type in self._calib_types:

            err_msg = (f"No matching master {calib_type} for dataId={data_id},"
                       f" calibDate={calib_date}.")

            doc_filter = {k: data_id[k] for k in self._matching_keys[calib_type]}
            doc_filter["datasetType"] = calib_type

            calib_ids = self.find(doc_filter)
            dates = [parse_date(_["calibDate"]) for _ in calib_ids]

            if len(dates) == 0:
                raise FileNotFoundError(err_msg)

            timediffs = [abs(calib_date - d) for d in dates]
            if min(timediffs) > self._validity:
                raise FileNotFoundError(err_msg)

            result[calib_type] = calib_ids[np.argmin(timediffs)]

        return result
