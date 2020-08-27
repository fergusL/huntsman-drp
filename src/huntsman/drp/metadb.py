"""Code to interface with the metadatabase."""
import os
import shutil
from contextlib import suppress
from abc import ABC, abstractmethod
from astropy.io import fits
from dateutil.parser import parse as parse_date

from huntsman.drp.fitsutil import FitsHeaderTranslator


class AbstractMetaDatabase(ABC):
    """

    """

    def __init__(self):
        pass

    @abstractmethod
    def query_files(self, *args, **kwargs):
        """
        Returns:
            List of filenames.
        """
        raise NotImplementedError

    @abstractmethod
    def query_dates(self, *args, **kwargs):
        """
        Returns:
            List of DateTime objects.
        """
        raise NotImplementedError

    def retrieve_files(self, directory, **kwargs):
        """
        Copy files listed by `query_files` to a directory.

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

    def _is_within_date_range(self, date, date_min=None, date_max=None):
        """Check if date is within range."""
        with suppress(TypeError):
            date = parse_date(date)
        if date_min is not None:
            with suppress(TypeError):
                date_min = parse_date(date_min)
            if date < date_min:
                return False
        if date_max is not None:
            with suppress(TypeError):
                date_max = parse_date(date_max)
            if date >= date_max:
                return False
        return True


class SimulatedMetaDatabase(AbstractMetaDatabase):
    """A simulated meta database for testing purposes. Should be replaced by simulated mongodb
    in future."""

    def __init__(self, data_directory, data_info, **kwargs):
        super().__init__(**kwargs)
        self._data_info = data_info
        self._translator = FitsHeaderTranslator()
        self._filenames = []
        for filename in os.listdir(data_directory):
            if filename.endswith(".fits"):
                self._filenames.append(os.path.join(data_directory, filename))

    def query_files(self, date_min=None, date_max=None):
        result = []
        for filename in self._filenames:
            header = fits.getheader(filename)
            date = self._translator.translate_dateObs(header)
            if self._is_within_date_range(date, date_min, date_max):
                result.append(filename)
        return result

    def query_dates(self, date_min=None, date_max=None):
        result = []
        for filename in self._filenames:
            header = fits.getheader(filename)
            date = parse_date(self._translator.translate_dateObs(header))
            if self._is_within_date_range(date, date_min, date_max):
                result.append(date)
        return result
