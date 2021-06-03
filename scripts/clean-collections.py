"""
Simple script to delete files in the DB that don't actually exist. Useful when migrating the DRP.
"""
import os

from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection

if __name__ == "__main__":

    collections = (RawExposureCollection(), MasterCalibCollection())

    for collection in collections:

        docs_to_delete = set()
        for doc in collection.find():

            # Check if the file actually exists
            filename = doc["filename"]
            if not os.path.isfile(filename):
                collection.logger.warning(f"File {filename} does not exist in {collection}."
                                          " Removing document.")
                docs_to_delete.add(doc)

        # Delete documents from collection
        collection.logger.info(f"Deleting {len(docs_to_delete)} documents.")
        collection.delete_many(docs_to_delete)
