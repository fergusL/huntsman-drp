"""Code to interface with the Huntsman mongo database."""
from copy import deepcopy
from datetime import timedelta
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import current_date, parse_date
from huntsman.drp.utils.query import Query
from huntsman.drp.utils.mongo import encode_mongo_query


class DataTable(HuntsmanBase):
    """ This class is used to interface with the mongodb. It is responsible for performing queries
    and inserting/updating/deleting documents, as well as validating new documents.
    """
    _required_columns = None
    _unique_columns = "filename",  # Required to identify a unique document

    def __init__(self, table_name, **kwargs):
        HuntsmanBase.__init__(self, **kwargs)

        self._date_key = self.config["mongodb"]["date_key"]
        self._table_name = table_name

        # Initialise the DB
        db_name = self.config["mongodb"]["db_name"]
        self._connect(db_name, self._table_name)

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
            self.logger.info(f"Connected to mongodb at {hostname}:{port}.")
        except ServerSelectionTimeoutError as err:
            self.logger.error(f"Unable to connect to mongodb at {hostname}:{port}.")
            raise err
        self._db = self._client[db_name]
        self._table = self._db[table_name]

    def count_documents(self, document_filter=None):
        """ Count the number of documents (matching document_filter criteria) in table.

        Parameters
        ----------
        document_filter : dict, optional
            A dictionary containing key, value pairs to be matched against other documents,
            by default None

        Returns
        -------
        int
            The number of matching documents in the table.
        """
        if document_filter is None:
            document_filter = {}
        return self._table.count_documents(document_filter)

    def find(self, document_filter=None, constraints=None, date_start=None, date_end=None,
             date=None, key=None):
        """Get data for one or more matches in the table.

        Parameters
        ----------
        document_filter : dict, optional
            A dictionary containing key, value pairs to be matched against other documents,
            by default None
        constraints : dict, optional
            A dictionary containing other search criteria which can include the mongo operators
            defined in the `huntsman.drp.utils.mongo.MONGO_OPERATORS`, by default None.
        date_start : object, optional
            Constrain query to a timeframe starting at date_start, by default None.
        date_end : object, optional
            Constrain query to a timeframe ending at date_end, by default None.
        date : object, optional
            Constrain query to specific date, by default None.
        key : str, optional
            Specify a specific key to be returned from the query (e.g. filename), by default None.

        Returns
        -------
        result: list or np.array
            Result of the query. If key is specified result will be a np.array(?)
        """
        if constraints is None:
            constraints = {}
        constraints = deepcopy(constraints)

        # Add date range to criteria if provided
        constraint = {}
        if date_start is not None:
            constraint.update({"greater_than_equal": parse_date(date_start)})
        if date_end is not None:
            constraint.update({"less_than": parse_date(date_end)})
        if date is not None:
            constraint.update({"equal": parse_date(date)})
        constraints[self._date_key] = constraint

        # Get the mongodb query
        query = Query(document=document_filter, constraints=constraints).to_mongo()
        self.logger.debug(f"Performing mongo query: {query}.")

        result = list(self._table.find(query))
        self.logger.debug(f"Query returned {len(result)} results.")

        if key is not None:
            result = np.array([d[key] for d in result])

        return result

    def find_one(self, *args, **kwargs):
        """ Find a single matching document, making sure only one document is matched.
        """
        documents = self.find(*args, **kwargs)
        if len(documents) > 1:
            raise RuntimeError("Matched with more than one document.")
        elif len(documents) == 0:
            raise RuntimeError("No matching documents.")
        return documents[0]

    def insert_one(self, document, overwrite=False):
        """Insert a new document into the table after ensuring it is valid and unique.

        Parameters
        ----------
        document : dict
            The document to be inserted into the table.
        overwrite : bool, optional
            If True override any existing document, by default False.

        """
        document = encode_mongo_query(document)

        # Check the required columns exist in the new document
        if self._required_columns is not None:
            for column_name in self._required_columns:
                if column_name not in document.keys():
                    raise ValueError(f"New document missing required column: {column_name}.")

        # Check there is at most one match in the table
        count = self._table.count_documents(document)
        if count == 1:
            if overwrite:
                self.delete(document)
            else:
                raise RuntimeError(f"Found existing document for {document} in {self}."
                                   " Pass overwrite=True to overwrite.")
        elif count != 0:
            raise RuntimeError(f"Multiple matches found for document in {self}: {document}.")

        # Insert the document
        self.logger.debug(f"Inserting new document into {self}: {document}.")
        self._table.insert_one(document)

    def update_one(self, document_filter, to_update, upsert=False):
        """ Update a single document in the table.

        Parameters
        ----------
        document_filter : dict, optional
            A dictionary containing key, value pairs used to identify the document to update,
            by default None
        to_update : dict
            The key, value pairs to update within the matched document.
        upsert : bool, optional
            If True perform the insert even if no matching documents are found,
            by default False

        https://docs.mongodb.com/manual/reference/operator/update/set/#up._S_set
        """
        document = encode_mongo_query(document_filter)
        to_update = encode_mongo_query(to_update)

        count = self._table.count_documents(document)
        if count > 1:
            raise RuntimeError(f"Multiple matches found for document in {self}: {document}.")
        elif (count == 0) and not upsert:
            raise RuntimeError(f"No matches found for document in {self}. Use upsert=True to"
                               " upsert.")
        self._table.update_one(document, {'$set': to_update}, upsert=upsert)

    def delete_one(self, document_filter):
        """Delete one document from the table.

        Parameters
        ----------
        document_filter : dict, optional
            A dictionary containing key, value pairs used to identify the document to delete,
            by default None
        """
        document = encode_mongo_query(document_filter)

        count = self._table.count_documents(document)
        if count > 1:
            raise RuntimeError(f"Multiple matches found for document in {self}: {document}.")
        elif (count == 0):
            raise RuntimeError(f"No matches found for document in {self}: {document}.")
        self._table.delete_one(document)

    def insert_many(self, documents, **kwargs):
        """Insert a new document into the table.

        Parameters
        ----------
        documents : list
            List of documents to be inserted into the table.
        """
        return [self.insert_one(d, **kwargs) for d in documents]

    def delete_many(self, documents, **kwargs):
        """ Delete one document from the table.

        Parameters
        ----------
        documents : list
            List of dictionaries that specify documents to be deleted from the table.

        """
        return [self.delete_one(d, **kwargs) for d in documents]

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

    def _get_unique_id(self, metadata):
        """ Return the unique identifier for the metadata.
        Args:
            metadata (abc.Mapping): The metadata.
        Returns:
            dict: The unique document identifier.
        """
        try:
            return encode_mongo_query({k: metadata[k] for k in self._unique_columns})
        # If there is a key missing, we will have to match with something in the table
        except KeyError:
            documents = self.query(metadata)
            if len(documents) > 1:
                raise RuntimeError(f"No unique ID for metadata: {metadata}.")
            return encode_mongo_query({k: documents[0][k] for k in self._unique_columns})


class ExposureTable(DataTable):
    """ Table to store metadata for Huntsman exposures. """

    def __init__(self, table_name="raw_data", **kwargs):
        super().__init__(table_name=table_name, **kwargs)
        self._required_columns = self.config["fits_header"]["required_columns"]

    def find_matching_raw_calibs(self, filename, days=None, **kwargs):
        """ Get matching calibs for a given file.
        """
        # Get metadata for this file
        document = self.find_one({"filename": filename})
        date = document[self._date_key]

        # Get date range for calibs
        if days is None:
            days = self.config["calibs"]["validity"]
        date_start = date - timedelta(days=days)
        date_end = date + timedelta(days=days)

        calib_metadata = []
        # Loop over raw calib dataTypes
        for calib_type, matching_columns in self.config["calibs"]["matching_columns"].items():

            # Matching raw calibs must satisfy these criteria
            document_filter = {m: document[m] for m in matching_columns}
            document_filter["dataType"] = calib_type

            # TODO: Implement screening
            documents = self.find(date_start=date_start, date_end=date_end,
                                  document_filter=document_filter, **kwargs)
            calib_metadata.extend(documents)

        return calib_metadata

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


class MasterCalibTable(DataTable):
    """ Table to store metadata for master calibs. """
    _required_columns = ("filename", "calibDate")  # TODO: Move to config

    def __init__(self, table_name="master_calib", **kwargs):
        super().__init__(table_name=table_name, **kwargs)
