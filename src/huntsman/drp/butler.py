import os
import shutil
from contextlib import suppress
from tempfile import TemporaryDirectory
import sqlite3

import lsst.daf.persistence as dafPersist
from lsst.daf.persistence.policy import Policy

from huntsman.drp.base import HuntsmanBase
import huntsman.drp.lsst_tasks as lsst
from huntsman.drp.datatable import MasterCalibTable
from huntsman.drp.utils.date import date_to_ymd, current_date
from huntsman.drp.utils.butler import get_files_of_type


class ButlerRepository(HuntsmanBase):
    _mapper = "lsst.obs.huntsman.HuntsmanMapper"
    _policy_filename = Policy.defaultPolicyFile("obs_huntsman", "HuntsmanMapper.yaml",
                                                relativePath="policy")
    _default_rerun = "default_rerun"

    def __init__(self, directory, calib_directory=None, initialise=True, **kwargs):
        super().__init__(**kwargs)

        # Specify directories
        self.butler_directory = directory
        if (calib_directory is None) and (directory is not None):
            calib_directory = os.path.join(directory, "CALIB")
        self._calib_directory = calib_directory
        self._calib_validity = self.config["calibs"]["validity"]

        # Load the policy file
        self._policy = Policy(self._policy_filename)

        # Initialise the bulter repository
        self.butler = None
        if initialise:
            self._initialise()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    @property
    def calib_directory(self):
        return self._calib_directory

    def get_filename(self, data_type, data_id):
        """ Get the filename for a data ID of data type.
        Args:
            data_type (str): The data type (raw, flat, bias etc.).
            data_id (dict): The data ID that uniquely specifies a file.
        Returns:
            str: The filename.
        """
        filename = self.butler.get(f"{data_type}_filename", data_id)
        self.logger.debug(f"{data_type} filename for data_id={data_id}: {filename}.")
        return filename

    def ingest_raw_data(self, filenames, **kwargs):
        """
        Ingest raw data into the repository.
        Args:
            filenames (iterable of str): The list of raw data filenames.
        """
        self.logger.debug(f"Ingesting {len(filenames)} files.")
        lsst.ingest_raw_data(filenames, butler_directory=self.butler_directory, **kwargs)

        # For some reason we need to make a new butler object...
        self.butler = dafPersist.Butler(inputs=self.butler_directory)

    def ingest_reference_catalogue(self, filenames):
        """
        Ingest the reference catalogue into the repository.
        Args:
            filenames (iterable of str): The list of filenames containing reference data.
        """
        self.logger.debug(f"Ingesting reference catalogue from {len(filenames)} files.")
        lsst.ingest_reference_catalogue(self.butler_directory, filenames)

    def make_master_flats(self, calib_date=None, rerun=None, **kwargs):
        """ Make master flats from ingested raw data.
        Args:
            calib_date (object, optional): The calib date to assign to the master calibs. If None
                (default), will use the current date.
            rerun (str): The name of the rerun.
        """
        return self._make_master_calibs("flat", calib_date=calib_date, rerun=rerun, **kwargs)

    def make_master_biases(self, calib_date=None, rerun=None, **kwargs):
        """ Make master biases from ingested raw data.
        Args:
            calib_date (object, optional): The calib date to assign to the master calibs. If None
                (default), will use the current date.
            rerun (str, optional): The name of the rerun.
        """
        return self._make_master_calibs("bias", calib_date=calib_date, rerun=rerun, **kwargs)

    def make_master_calibs(self, calib_date=None, rerun=None, skip_bias=False, **kwargs):
        """ Make master calibs from ingested raw calib data.
        Args:
            calib_date (object, optional): The calib date to assign to the master calibs. If None
                (default), will use the current date.
            rerun (str, optional): The name of the rerun. If None (default), use default rerun.
            skip_bias (bool, optional): Skip creation of master biases? Default False.
        """
        if calib_date is None:
            calib_date = current_date()
        if rerun is None:
            rerun = self._default_rerun
        if not skip_bias:
            self.make_master_biases(calib_date, rerun, **kwargs)
        self.make_master_flats(calib_date, rerun, **kwargs)

    def ingest_master_calibs(self, calib_type, filenames, validity=None):
        """ Ingest the master calibs into the butler repository.
        Args:
            calib_type (str): The calib dataset type (e.g. bias, flat).
            filenames (list of str): The files to ingest.
            validity (int, optional): How many days the calibs remain valid for. Default 1000.
        """
        if validity is None:
            validity = self._calib_validity
        self.logger.info(f"Ingesting {len(filenames)} master {calib_type} calibs with validity="
                         f"{validity}.")
        lsst.ingest_master_calibs(calib_type, filenames, self.butler_directory,
                                  self.calib_directory, validity=validity)

    def archive_master_calibs(self):
        """ Copy the master calibs from this Butler repository into the calib archive directory
        and insert the metadata into the master calib metadatabase.
        """
        calib_archive_dir = self.config["directories"]["archive"]["master_calibs"]
        calib_datatable = MasterCalibTable(config=self.config, logger=self.logger)

        for calib_type in ("flat", "bias"):
            # Retrieve filenames and dataIds for all files of this type
            data_ids, filenames = get_files_of_type(f"calibrations.{calib_type}",
                                                    directory=self.calib_directory,
                                                    policy=self._policy)
            for metadata, filename in zip(data_ids, filenames):
                # Create the filename for the archived copy
                archived_filename = os.path.join(calib_archive_dir,
                                                 os.path.relpath(filename, self.calib_directory))
                # Copy the file into the calib archive
                self.logger.debug(f"Copying {filename} to {archived_filename}.")
                os.makedirs(os.path.dirname(archived_filename), exist_ok=True)
                shutil.copy(filename, archived_filename)

                # Insert the metadata into the calib database
                metadata["filename"] = archived_filename
                metadata["datasetType"] = calib_type
                calib_datatable.insert_one(metadata)

    def query_calib_metadata(self, table):
        """
        Query the ingested calibs. TODO: Replace with the "official" Butler version.
        Args:
            table (str): Table name. Can either be "flat" or "bias".
        Returns:
            list of dict: The query result in column: value.
        """
        # Access the sqlite DB
        conn = sqlite3.connect(os.path.join(self.calib_directory, "calibRegistry.sqlite3"))
        c = conn.cursor()
        # Query the calibs
        result = c.execute(f"SELECT * from {table}")
        result_dict = []
        for row in result:
            d = {}
            for idx, col in enumerate(c.description):
                d[col[0]] = row[idx]
            result_dict.append(d)
        c.close()
        return result_dict

    def _initialise(self):
        """Initialise a new butler repository."""
        # Add the mapper file to each subdirectory, making directory if necessary
        for subdir in ["", "CALIB"]:
            dir = os.path.join(self.butler_directory, subdir)
            with suppress(FileExistsError):
                os.mkdir(dir)
            filename_mapper = os.path.join(dir, "_mapper")
            with open(filename_mapper, "w") as f:
                f.write(self._mapper)
        self.butler = dafPersist.Butler(inputs=self.butler_directory)

    def _make_master_calibs(self, calib_type, calib_date, rerun, nodes=1, procs=1, ingest=True):
        """ Use the LSST stack to create master calibs.
        Args:
            calib_type (str): The dataset type, e.g. bias, flat.
            calib_date (date): The date to associate with the master calibs.
            rerun (str): The rerun name.
            nodes (int, optional): Run on this many nodes. Default=1.
            proces (int, optional): Run on this many processes per node. Default=1.
            ingest (bool, optional): If True (default), ingest the master calibs into the butler
                repository.
        Returns:
            list of str: The filenames of the master calibs.
        """
        calib_date = date_to_ymd(calib_date)

        # Get dataIds for the raw calib frames
        keys = list(self.butler.getKeys("raw").keys())
        metalist = self.butler.queryMetadata("raw", format=keys, dataId={'dataType': calib_type})
        data_ids = [{k: v for k, v in zip(keys, m)} for m in metalist]

        # Construct the master bias frames
        self.logger.debug(f"Creating master {calib_type} frames for calibDate={calib_date} with"
                          f" dataIds: {data_ids}.")
        lsst.make_master_calibs(calib_type, data_ids, butler_repository=self, rerun=rerun,
                                nodes=nodes, procs=procs, calib_date=calib_date)

        # Get filenames of the master calibs
        calib_dir = os.path.join(self.butler_directory, "rerun", rerun)
        _, filenames = get_files_of_type(f"calibrations.{calib_type}", directory=calib_dir,
                                         policy=self._policy)

        # Ingest the masters into the butler repo
        if ingest:
            self.ingest_master_calibs(calib_type, filenames)

        return filenames


class TemporaryButlerRepository(ButlerRepository):
    """ Create a new Butler repository in a temporary directory."""

    def __init__(self, **kwargs):
        super().__init__(directory=None, initialise=False, **kwargs)

    def __enter__(self):
        """Create temporary directory and initialise as a Bulter repository."""
        self._tempdir = TemporaryDirectory()
        self.butler_directory = self._tempdir.name
        self._initialise()
        return self

    def __exit__(self, *args, **kwargs):
        """Close temporary directory."""
        self.butler = None
        self._tempdir.cleanup()
        self.butler_directory = None

    @property
    def calib_directory(self):
        if self.butler_directory is None:
            return None
        return os.path.join(self.butler_directory, "CALIB")
