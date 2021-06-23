"""Code to interface with the Huntsman mongo database."""
import os
import shutil
from contextlib import suppress
from datetime import timedelta
from urllib.parse import quote_plus

import numpy as np
import pymongo
from pymongo.errors import ServerSelectionTimeoutError, DuplicateKeyError

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import current_date, parse_date, date_to_ymd
from huntsman.drp.document import Document, RawExposureDocument, CalibDocument
from huntsman.drp.utils.mongo import encode_mongo_filter, mongo_logical_or, mongo_logical_and
from huntsman.drp.utils.ingest import METRIC_SUCCESS_FLAG
from huntsman.drp.lsst.utils.calib import get_calib_filename


class Collection(HuntsmanBase):
    """ This class is used to interface with the mongodb. It is responsible for performing queries
    and inserting/updating/deleting documents, as well as validating new documents.
    """
    _unique_keys = None

    def __init__(self, db_name=None, collection_name=None, **kwargs):
        super().__init__(**kwargs)

        cfg = self.config["mongodb"]
        if not db_name:
            db_name = cfg["db_name"]

        # Get collection name from config if not explicitly provided
        if not collection_name:
            collection_name = cfg["collections"][self.__class__.__name__]["name"]

        self._db_name = db_name
        self.collection_name = collection_name

        # Initialise the DB
        db_name = self.config["mongodb"]["db_name"]
        self._connect()

    def __str__(self):
        return f"{self.collection_name} ({self.__class__.__name__})"

    # Public methods

    def count_documents(self, *args, **kwargs):
        """ Count the number of matching documents in the collection.
        Args:
            *args, **kwargs: Parsed to self.find.
        Returns:
            int: The number of matching documents in the collection.
        """
        return len(self.find(*args, **kwargs))

    def find(self, document_filter=None, date_min=None, date_max=None, date=None, key=None,
             screen=False, quality_filter=False, limit=None):
        """Get data for one or more matches in the table.
        Args:
            document_filter (dict, optional): A dictionary containing key, value pairs to be
                matched against other documents, by default None
            date_min (object, optional): Constrain query to a timeframe starting at date_min,
                by default None.
            date_max (object, optional): Constrain query to a timeframe ending at date_max, by
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
            limit (int): Limit the number of returned documents to this amount.
        Returns:
            result (list): List of DataIds or key values if key is specified.
        """
        document_filter = Document(document_filter, copy=True)
        with suppress(KeyError):
            del document_filter["date_modified"]  # This might change so don't match with it

        # Add date range to criteria if provided
        date_constraint = {}

        if date_min is not None:
            date_constraint.update({"greater_than_equal": parse_date(date_min)})
        if date_max is not None:
            date_constraint.update({"less_than": parse_date(date_max)})
        if date is not None:
            date_constraint.update({"equal": parse_date(date)})

        if date_constraint:
            document_filter.update({self._date_key: date_constraint})

        # Screen the results if necessary
        if screen:
            document_filter[METRIC_SUCCESS_FLAG] = True  # TODO: Move to raw exposure table

        mongo_filter = document_filter.to_mongo()

        # Apply quality cuts
        if quality_filter:
            mongo_quality_filter = self._get_quality_filter()
            if mongo_quality_filter:
                mongo_filter = mongo_logical_and([mongo_filter, mongo_quality_filter])

        self.logger.debug(f"Performing mongo find operation with filter: {mongo_filter}.")

        if limit is None:
            limit = 0
        cursor = self._collection.find(mongo_filter, {"_id": False}).limit(limit)
        documents = list(cursor)

        self.logger.debug(f"Find operation returned {len(documents)} results.")

        if key is not None:
            return [d[key] for d in documents]

        # Skip validation to speed up - inserted documents should already be valid
        return [self._document_type(d, validate=False, config=self.config) for d in documents]

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

    def insert_one(self, document):
        """ Insert a new document into the table after ensuring it is valid and unique.
        Args:
            document (dict): The document to be inserted into the table.
        """
        # Check the required columns exist in the new document
        doc = self._prepare_doc_for_insert(document)

        # Insert the document
        # Uniqueness is verified implicitly
        self.logger.debug(f"Inserting document into {self}: {doc}.")
        self._collection.insert_one(doc.to_mongo())

    def replace_one(self, document_filter, replacement, **kwargs):
        """ Replace a matching document with a new one.
        Args:
            document_filter (Document): dictionary containing key, value pairs used to identify
                the document to replace.
            replacement (Document): The document to replace with.
            **kwargs: Parsed to pymongo replace_one.
        Raises:
            RuntimeError: If document filter matches with more than one document.
        """
        document_filter = Document(document_filter)

        # Make sure the filter matches with at most one doc
        if self.count_documents(document_filter) > 1:
            raise RuntimeError(f"Document filter {document_filter} matches with multiple documents"
                               f" in {self}.")

        mongo_filter = document_filter.to_mongo()
        mongo_doc = self._prepare_doc_for_insert(replacement).to_mongo()  # Implicit validation

        self.logger.debug(f"Replacing {mongo_filter} with {mongo_doc}")

        self._collection.replace_one(mongo_filter, mongo_doc, **kwargs)

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
        document_filter = Document(document_filter, copy=True)
        with suppress(KeyError):
            del document_filter["date_modified"]  # This might change so don't match with it

        count = self.count_documents(document_filter)
        if count > 1:
            raise RuntimeError(f"Multiple matches found for document in {self}: {document_filter}.")

        elif count == 0:
            if upsert:
                self.insert_one(to_update)
                return
            else:
                raise RuntimeError(f"No matches found for document {document_filter} in {self}. Use"
                                   " upsert=True to upsert.")

        to_update = Document(to_update)
        to_update["date_modified"] = current_date()

        # Use flattened version (dot notation) for nested updates to work properly
        mongo_update = to_update.to_mongo(flatten=True)

        self.logger.debug(f"Updating document with: {mongo_update}")
        self._collection.update_one(document_filter, {'$set': mongo_update}, upsert=False)

    def delete_one(self, document_filter, force=False):
        """Delete one document from the table.
        Args:
            document_filter (dict, optional): A dictionary containing key, value pairs used to
                identify the document to delete, by default None
            force (bool, optional): If True, ignore checks and delete all matching documents.
                Default False.
        """
        document_filter = Document(document_filter, validate=False)
        mongo_filter = document_filter.to_mongo()

        if not force:
            count = self.count_documents(document_filter)
            if count > 1:
                raise RuntimeError(f"Multiple matches found for document in {self}:"
                                   f" {document_filter}.")
            elif (count == 0):
                raise RuntimeError(f"No matches found for document in {self}: {document_filter}.")

        self.logger.debug(f"Deleting {document_filter} from {self}.")

        self._collection.delete_one(mongo_filter)

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
        self.logger.debug(f"Deleting {len(documents)} documents from {self}.")

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
        date_min = date_now - timedelta(days=days, hours=hours, seconds=seconds)
        return self.find(date_min=date_min, **kwargs)

    def delete_all(self, really=False, **kwargs):
        """ Delete all documents from the collection. """
        if not really:
            raise RuntimeError("If you really want to do this, parse really=True.")
        docs = self.find()
        self.delete_many(docs, **kwargs)

    # Private methods

    def _connect(self):
        """ Initialise the database.
        Args:
            db_name (str): The name of the (mongo) database.
            collection_name (str): The name of the table (mongo collection).
        """
        # Connect to the mongodb
        hostname = self.config["mongodb"]["hostname"]
        port = self.config["mongodb"]["port"]

        if "username" in self.config["mongodb"].keys():
            username = quote_plus(self.config["mongodb"]["username"])
            password = quote_plus(self.config["mongodb"]["password"])
            uri = f"mongodb://{username}:{password}@{hostname}/{self._db_name}?ssl=true"
            self._client = pymongo.MongoClient(uri)
        else:
            self._client = pymongo.MongoClient(hostname, port)
        try:
            self._client.server_info()
            self.logger.info(f"{self} connected to mongodb at {hostname}:{port}.")
        except ServerSelectionTimeoutError as err:
            self.logger.error(f"Unable to connect {self} to mongodb at {hostname}:{port}.")
            raise err

        self._db = self._client[self._db_name]
        self._collection = self._db[self.collection_name]

        # Define which keys identify unique documents
        self._set_unique_keys()

    def _set_unique_keys(self):
        """ Define the set of keys (if any) that identify a unique document.
        This approach leverages mongdb's server-side locking mechanism to ensure thread-safety on
        inserts.
        See: https://docs.mongodb.com/manual/core/index-unique
        """
        cfg = self.config["mongodb"]["collections"].get(self.__class__.__name__, {})

        unique_keys = cfg.get("unique_keys", None)
        if unique_keys:
            self._collection.create_index([(k, pymongo.ASCENDING) for k in unique_keys],
                                          unique=True)

    def _get_quality_filter(self):
        """ Return the Query object corresponding to quality cuts. """
        raise NotImplementedError

    def _prepare_doc_for_insert(self, document):
        """ Prepare a document to be inserted into the database.
        Args:
            document (Document or dict): The document to prepare.
        Returns:
            Document: The prepared document of the appropriate type for this collection.
        """
        doc = self._document_type(document, copy=True, unflatten=True, config=self.config)

        # Add date records
        doc["date_created"] = current_date()
        doc["date_modified"] = current_date()

        return doc


class RawExposureCollection(Collection):
    """ Table to store metadata for Huntsman exposures. """

    _document_type = RawExposureDocument

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # Public methods

    def insert_one(self, document, *args, **kwargs):
        """ Override to make sure the document does not clash with an fpacked version.
        Args:
            document (RawExposureDocument): The document to insert.
            *args, **kwargs: Parsed to super().insert_one
        Raises:
            DuplicateKeyError: If a .fz / .fits duplicate already exists.
        """
        doc = self._document_type(document, copy=True, config=self.config)
        filename = doc["filename"]

        if filename.endswith(".fits"):
            if self.find({"filename": filename + ".fz"}):
                raise DuplicateKeyError(f"Tried to insert {filename} but a .fz version exists.")

        elif filename.endswith(".fits.fz"):
            if self.find({"filename": filename.strip(".fz")}):
                raise DuplicateKeyError(f"Tried to insert {filename} but a .fits version exists.")

        return super().insert_one(document, *args, **kwargs)

    def get_matching_raw_calibs(self, calib_document, calib_date, validity=None):
        """ Return matching set of calib IDs for a given data_id and calib_date.
        Args:
            calib_document (CalibDocument): The calib document to match with.
            calib_date (object): An object that can be interpreted as a date.
            validity (datetime.timedelta): The validity of the calibs.
        Returns:
            list of RawExposureDocument: The matching raw calibs ordered by increasing time diff.
        """
        if validity is None:
            validity = timedelta(days=self.config["calibs"]["validity"])

        # Make the document filter
        dataset_type = calib_document["datasetType"]
        matching_keys = self.config["calibs"]["matching_columns"][dataset_type]

        doc_filter = {k: calib_document[k] for k in matching_keys}

        # Add dataType to doc filter
        doc_filter["dataType"] = dataset_type

        # Add valid date range to query
        calib_date = parse_date(calib_date)
        date_min = calib_date - validity
        date_max = calib_date + validity

        # Do the query
        documents = self.find(doc_filter, date_min=date_min, date_max=date_max)
        self.logger.debug(f"Found {len(documents)} matching raw calib documents for"
                          f" {calib_document} at {calib_date}.")

        # Sort by time difference in increasing order
        # This makes it easy to select only the nearest matches using indexing
        timedeltas = [abs(d["date"] - calib_date) for d in documents]
        indices = np.argsort(timedeltas)
        documents = [documents[i] for i in indices]

        return documents

    def get_calib_docs(self, calib_date, documents=None, validity=None):
        """ Get all possible CalibDocuments from a set of RawExposureDocuments.
        Args:
            calib_date (object): The calib date.
            documents (list of RawExposureDocument, optional): The list of documents to process.
                If not provided, will lookup the appropriate documents from the collection.
            validity (datetime.timedelta): The validity of the calibs.
        Returns:
            set of CalibDocument: The calb documents.
        """
        data_types = self.config["calibs"]["types"]
        if validity is None:
            validity = timedelta(days=self.config["calibs"]["validity"])

        calib_date = parse_date(calib_date)
        date_min = calib_date - validity
        date_max = calib_date + validity

        # Get metadata for all raw calibs that are valid for this date
        if documents is None:
            documents = self.find({"dataType": {"in": data_types}}, date_min=date_min,
                                  date_max=date_max, screen=True, quality_filter=True)
        else:
            documents = [d for d in documents if d["dataType"] in data_types]

        calib_docs = set([self.raw_doc_to_calib_doc(d, calib_date) for d in documents])

        self.logger.info(f"Found {len(calib_docs)} calibIds for calib_date={calib_date}.")

        return calib_docs

    def raw_doc_to_calib_doc(self, document, calib_date):
        """ Convert a RawExposureDocument into its corresponding CalibDocument.
        Args:
            document (RawExposureDocument): The raw calib document.
            calib_date (object): The calib date.
        Returns:
            CalibDocument: The matching calib document.
        """
        calib_type = document["dataType"]

        # Get minimal calib metadata
        keys = self.config["calibs"]["matching_columns"][calib_type]
        calib_dict = {k: document[k] for k in keys}

        # Add extra required metadata
        calib_dict["calibDate"] = date_to_ymd(calib_date)
        calib_dict["datasetType"] = calib_type
        calib_dict["filename"] = get_calib_filename(calib_dict, config=self.config)

        return CalibDocument(calib_dict)

    def clear_calexp_metrics(self):
        """ Clear all calexp metrics from the collection.
        This is useful e.g. to trigger them for reprocessing. """

        self.logger.info(f"Clearing all calexp metrics from {self}.")

        self._collection.update_many({}, {"$unset": {"metrics.calexp": ""}})

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

    _document_type = CalibDocument

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_matching_calibs(self, document):
        """ Return best matching set of calibs for a given document.
        Args:
            document (RawExposureDocument): The document to match with.
        Returns:
            dict: A dict of datasetType: CalibDocument.
        Raises:
            FileNotFoundError: If there is no matching calib of any type.
            TODO: Make new MissingCalibError and raise instead.
        """
        self.logger.debug(f"Finding best matching calibs for {document}.")

        validity = timedelta(days=self.config["calibs"]["validity"])
        matching_keys = self.config["calibs"]["matching_columns"]

        # Specify valid date range
        date = parse_date(document["dateObs"])
        date_min = date - validity
        date_max = date + validity

        best_calibs = {}
        for calib_type in self.config["calibs"]["types"]:

            doc_filter = {k: document[k] for k in matching_keys[calib_type]}
            doc_filter["datasetType"] = calib_type

            # Find matching docs within valid date range
            calib_docs = self.find(doc_filter, date_min=date_min, date_max=date_max)

            # If none within valid range, log a warning and proceed
            if len(calib_docs) == 0:
                self.logger.warning(f"Best {calib_type} outside valid date range for {document}.")
                calib_docs = self.find(doc_filter)

            # If there are still no matches, raise an error
            if len(calib_docs) == 0:
                raise FileNotFoundError(f"No matching master {calib_type} for {doc_filter}.")

            dates = [parse_date(_["calibDate"]) for _ in calib_docs]
            timediffs = [abs(date - d) for d in dates]

            # Choose the one with the nearest date
            best_calibs[calib_type] = calib_docs[np.argmin(timediffs)]

        return best_calibs

    def archive_master_calib(self, filename, metadata):
        """ Copy the FITS files into the archive directory and update the entry in the DB.
        Args:
            filename (str): The filename of the calib to archive, which is copied into the archive
                dir.
            metadata (abc.Mapping): The calib metadata to be stored in the document.
        """
        # Use the archived filename for the mongo document
        archived_filename = get_calib_filename(metadata, config=self.config)

        # Copy the file into the calib archive, overwriting if necessary
        self.logger.debug(f"Copying {filename} to {archived_filename}.")
        os.makedirs(os.path.dirname(archived_filename), exist_ok=True)
        shutil.copy(filename, archived_filename)

        # Update the document before archiving
        metadata = metadata.copy()
        metadata["filename"] = archived_filename

        # Insert the metadata into the calib database
        # Use replace operation with upsert because old document may already exist
        self.replace_one({"filename": archived_filename}, metadata, upsert=True)
