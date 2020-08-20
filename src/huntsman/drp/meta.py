"""Code to interface with the metadatabase."""
import os
import shutil


class MetaDatabase():
    """

    """

    def __init__(self):
        pass

    def query_files(self, date_min=None, date_max=None, data_type=None):
        """
        Arguments:

        Returns:
            List of filenames.
        """

    def retrieve_files(self, directory, **kwargs):
        """
        Copy files listed by `qery_files` to a directory.

        Arguments:
            directory (str): The directory to copy files into.
            **kwargs: Passed to `MetaDatabase.query_files`
        """
        # Query the files to copy
        filenames = self.query_files(**kwargs)

        # Make sure directory exists
        os.makedirs(directory, exist_ok=True)

        # Copy files
        for filename in filenames:
            basename = os.path.basename(filename)
            newfilename = os.path.join(directory, basename)
            shutil.copyfile(filename, newfilename)
