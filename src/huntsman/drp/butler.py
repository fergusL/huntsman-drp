import os
import shutil
from contextlib import suppress
from tempfile import TemporaryDirectory
import sqlite3

import lsst.daf.persistence as dafPersist
from lsst.daf.persistence.policy import Policy

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.lsst import tasks
from huntsman.drp.datatable import MasterCalibTable
from huntsman.drp.refcat import TapReferenceCatalogue
from huntsman.drp.utils.date import date_to_ymd, current_date_ymd
import huntsman.drp.lsst.utils.butler as utils
from huntsman.drp.fitsutil import read_fits_header


class ButlerRepository(HuntsmanBase):
    _mapper = "lsst.obs.huntsman.HuntsmanMapper"
    _policy_filename = Policy.defaultPolicyFile("obs_huntsman", "HuntsmanMapper.yaml",
                                                relativePath="policy")
    _ra_key = "RA-MNT"
    _dec_key = "DEC-MNT"  # TODO: Move to config

    def __init__(self, directory, calib_directory=None, initialise=True, **kwargs):
        super().__init__(**kwargs)

        self.butler_directory = directory

        if (calib_directory is None) and (directory is not None):
            calib_directory = os.path.join(directory, "CALIB")
        self._calib_directory = calib_directory
        self._calib_validity = self.config["calibs"]["validity"]

        if self.butler_directory is None:
            self._refcat_filename = None
        else:
            self._refcat_filename = os.path.join(self.butler_directory, "refcat_raw",
                                                 "refcat_raw.csv")

        # Load the policy file
        self._policy = Policy(self._policy_filename)

        # Initialise the bulter repository
        self._butlers = {}  # One butler for each rerun
        if initialise:
            self._initialise()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    @property
    def calib_directory(self):
        return self._calib_directory

    @property
    def status(self):
        # TODO: Information here about number of ingested files etc
        raise NotImplementedError

    def get_butler(self, rerun=None):
        """ Get a butler object for a given rerun.
        Args:
            rerun (str, optional): The rerun name. If None, the butler is created for the root
                butler directory.
        Returns:
            butler: The butler object.
        """
        try:
            return self._butlers[rerun]
        except KeyError:
            self.logger.debug(f"Creating new bulter object for rerun={rerun}.")
            if rerun is None:
                butlerdir = self.butler_directory
            else:
                butlerdir = os.path.join(self.butler_directory, "rerun", rerun)
            os.makedirs(butlerdir, exist_ok=True)
            self._butlers[rerun] = dafPersist.Butler(inputs=butlerdir)
        return self._butlers[rerun]

    def get(self, datasetType, dataId, rerun=None, **kwargs):
        """ Get a dataset from the butler repository.
        Args:
            datasetType (str): The dataset type (raw, flat, bias etc.).
            dataId (dict): The data ID that uniquely specifies a file.
            rerun (str, optional): The rerun name. If None (default), will use the root butler
                directory.
        Returns:
            object: The dataset.
        """
        butler = self.get_butler(rerun=rerun)
        return butler.get(datasetType, dataId=dataId, **kwargs)

    def get_keys(self, datasetType, **kwargs):
        """ Get set of keys required to uniquely identify ingested data.
        Args:
            datasetType (str): The dataset type (raw, flat, bias etc.).
        Returns:
            list of str: A list of keys.
        """
        butler = self.get_butler(**kwargs)
        return list(butler.getKeys(datasetType))

    def get_filename(self, datasetType, dataId, **kwargs):
        """ Get the filename for a data ID of data type.
        Args:
            datasetType (str): The dataset type (raw, flat, bias etc.).
            dataId (dict): The data ID that uniquely specifies a file.
        Returns:
            str: The filename.
        """
        return self.get(datasetType + "_filename", dataId=dataId, **kwargs)

    def get_data_ids(self, datasetType, dataId=None, extra_keys=None, rerun=None):
        """ Get ingested dataIds for a given datasetType.
        Args:
            datasetType (str): The datasetType (raw, bias, flat etc.).
            dataId (dict, optional): A complete or partial dataId to match with.
            extra_keys (list, optional): List of additional keys to be included in the dataIds.
        Returns:
            list of dict: A list of dataIds.
        """
        butler = self.get_butler(rerun=rerun)
        return utils.get_data_ids(butler=butler, datasetType=datasetType, dataId=dataId,
                                  extra_keys=extra_keys)

    def get_calexps(self, rerun="default", dataType="science", extra_keys=None, **kwargs):
        """ Convenience function to get the calexps produced in a given rerun.
        Args:

        Returns:
            list of lsst.afw.image.exposure: The list of calexp objects.
        """
        data_ids = self.get_data_ids("calexp", dataId={"dataType": dataType}, rerun=rerun,
                                     extra_keys=extra_keys)
        calexps = [self.get("calexp", dataId=d, rerun=rerun, **kwargs) for d in data_ids]
        if len(calexps) != len(data_ids):
            raise RuntimeError("Number of dataIds does not match the number of calexps.")

        return calexps, data_ids

    def ingest_raw_data(self, filenames, **kwargs):
        """ Ingest raw data into the repository.
        Args:
            filenames (iterable of str): The list of raw data filenames.
        """
        self.logger.debug(f"Ingesting {len(filenames)} files.")
        tasks.ingest_raw_data(filenames, butler_directory=self.butler_directory, **kwargs)

    def make_master_calibs(self, calib_date=None, rerun="default", skip_bias=False,
                           skip_dark=False, **kwargs):
        """ Make master calibs from ingested raw calib data.
        Args:
            calib_date (object, optional): The calib date to assign to the master calibs. If None
                (default), will use the current date.
            rerun (str, optional): The name of the rerun. If None (default), use default rerun.
            skip_bias (bool, optional): Skip creation of master biases? Default False.
            skip_dark (bool, optional): Skip creation of master darks? Default False.
        """
        if calib_date is None:
            calib_date = current_date_ymd()
        else:
            calib_date = date_to_ymd(calib_date)

        for calib_type in ("bias", "dark", "flat"):
            if skip_bias and calib_type == "bias":
                continue
            if skip_dark and calib_type == "dark":
                continue
            self.logger.info(f"Creating master {calib_type} frames for calib_date={calib_date}.")
            self._make_master_calibs(calib_type, calib_date=calib_date, rerun=rerun, **kwargs)
            self._check_master_calibs(calib_type)

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
        tasks.ingest_master_calibs(calib_type, filenames, self.butler_directory,
                                   self.calib_directory, validity=validity)

    def archive_master_calibs(self):
        """ Copy the master calibs from this Butler repository into the calib archive directory
        and insert the metadata into the master calib metadatabase.
        """
        archive_dir = self.config["directories"]["archive"]
        calib_datatable = MasterCalibTable(config=self.config, logger=self.logger)

        for calib_type in self.config["calibs"]["types"]:
            # Retrieve filenames and dataIds for all files of this type
            data_ids, filenames = utils.get_files_of_type(f"calibrations.{calib_type}",
                                                          directory=self.calib_directory,
                                                          policy=self._policy)
            for metadata, filename in zip(data_ids, filenames):
                # Create the filename for the archived copy
                archived_filename = os.path.join(archive_dir,
                                                 os.path.relpath(filename, self.calib_directory))
                # Copy the file into the calib archive
                self.logger.debug(f"Copying {filename} to {archived_filename}.")
                os.makedirs(os.path.dirname(archived_filename), exist_ok=True)
                shutil.copy(filename, archived_filename)

                # Insert the metadata into the calib database
                metadata["filename"] = archived_filename
                metadata["datasetType"] = calib_type
                calib_datatable.insert_one(metadata, overwrite=True)

    def query_calib_metadata(self, datasetType, keys_ignore=None):
        """ Query the ingested calibs. TODO: Replace with the "official" Butler version.
        Args:
            datasetType (str): The dataset type (e.g. bias, dark, flat).
            keys_ignore (list of str, optional): If provided, drop these keys from result.
        Returns:
            list of dict: The query result in column: value.
        """
        # Access the sqlite DB
        conn = sqlite3.connect(os.path.join(self.calib_directory, "calibRegistry.sqlite3"))
        c = conn.cursor()
        # Query the calibs
        result = c.execute(f"SELECT * from {datasetType}")
        metadata_list = []
        for row in result:
            d = {}
            for idx, col in enumerate(c.description):
                d[col[0]] = row[idx]
            metadata_list.append(d)
        c.close()
        if keys_ignore is not None:
            keys_keep = [k for k in metadata_list[0].keys() if k not in keys_ignore]
            metadata_list = [{k: _[k] for k in keys_keep} for _ in metadata_list]
        return metadata_list

    def make_reference_catalogue(self, ingest=True, **kwargs):
        """ Make the reference catalogue for the ingested science frames.
        Args:
            ingest (bool, optional): If True (default), ingest refcat into butler repo.
        """
        butler = self.get_butler(**kwargs)

        # Get the filenames of ingested images
        data_ids, filenames = utils.get_files_of_type("exposures.raw", self.butler_directory,
                                                      policy=self._policy)
        # Use the FITS header sto retrieve the RA/Dec info
        ra_list = []
        dec_list = []
        for data_id, filename in zip(data_ids, filenames):
            data_type = butler.queryMetadata("raw", ["dataType"], dataId=data_id)[0]
            if data_type == "science":  # Only select science files
                header = read_fits_header(filename, ext="all")  # Use all as .fz extension is lost
                ra_list.append(header[self._ra_key])
                dec_list.append(header[self._dec_key])
        self.logger.debug(f"Creating reference catalogue for {len(ra_list)} science frames.")

        # Make the reference catalogue
        tap = TapReferenceCatalogue(config=self.config, logger=self.logger)
        tap.make_reference_catalogue(ra_list, dec_list, filename=self._refcat_filename)

        # Ingest into the repository
        if ingest:
            self.ingest_reference_catalogue(filenames=(self._refcat_filename,))

    def ingest_reference_catalogue(self, filenames):
        """ Ingest the reference catalogue into the repository.
        Args:
            filenames (iterable of str): The list of filenames containing reference data.
        """
        self.logger.debug(f"Ingesting reference catalogue from {len(filenames)} files.")
        tasks.ingest_reference_catalogue(self.butler_directory, filenames)

    def make_calexps(self, rerun="default", **kwargs):
        """ Make calibrated exposures (calexps) using the LSST stack.
        Args:
            rerun (str, optional): The name of the rerun. Default is "default".
            procs (int, optional): Run on this many processes (default 1).
        """
        # Get dataIds for the raw science frames
        data_ids = self.get_data_ids(datasetType="raw", dataId={'dataType': "science"},
                                     extra_keys=["filter"])

        self.logger.info(f"Making calexps from {len(data_ids)} dataIds.")

        # Process the science frames
        tasks.make_calexps(data_ids, rerun=rerun, butler_directory=self.butler_directory,
                           calib_directory=self.calib_directory, **kwargs)

        # Check if we have the right number of calexps
        if not len(self.get_calexps(rerun=rerun)[0]) == len(data_ids):
            raise RuntimeError("Number of calexps does not match the number of dataIds.")

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

    def _check_master_calibs(self, datasetType, raise_error=True):
        """ Check that the correct number of master calibs have been created following a call
        to make_master_calibs. This function compares the set of calibIds that should be ingested
        (using ingested raw calibs) to the calibIds that actually exist and are ingested.
        Args:
            datasetType (str): The dataset type. Should be a valid calib dataset type (e.g. bias).
            raise_error (bool, optional): If True (default), an error will be raised if there
                are missing calibIds. Else, a warning is generated.
        """
        butler = self.get_butler()  # Use root butler

        keys_ignore = ["id", "calibDate", "validStart", "validEnd"]

        extra_keys = []
        if datasetType == "flat":
            extra_keys.append("filter")  # TODO: Get this info somewhere else

        # Get dataIds of raw ingested calibs
        raw_ids = self.get_data_ids(datasetType="raw", dataId={'dataType': datasetType},
                                    extra_keys=extra_keys)

        # Get calibIds of master calibs that *should* be ingested
        calib_ids_required = utils.data_id_to_calib_id(datasetType, raw_ids, butler=butler,
                                                       keys_ignore=keys_ignore)

        # Get calibIds of ingested master calibs
        calib_ids_ingested = self.query_calib_metadata(datasetType, keys_ignore=keys_ignore)

        # Check for missing master calibs
        missing_ids = utils.get_missing_data_ids(calib_ids_ingested, calib_ids_required)

        # Handle result
        if len(missing_ids) > 0:
            msg = f"{len(missing_ids)} missing master {datasetType} calibs: {missing_ids}."
            if raise_error:
                self.logger.error(msg)
                raise FileNotFoundError(msg)
            else:
                self.logger.warning(msg)
        else:
            self.logger.debug(f"No missing {datasetType} calibs detected.")

    def _make_master_calibs(self, calib_type, calib_date, rerun, ingest=True, **kwargs):
        """ Use the LSST stack to create master calibs.
        Args:
            calib_type (str): The dataset type, e.g. bias, flat.
            calib_date (date): The date to associate with the master calibs.
            rerun (str): The rerun name.
            ingest (bool, optional): If True (default), ingest the master calibs into the butler
                repository.
        Returns:
            list of str: The filenames of the master calibs.
        """
        butler = self.get_butler()  # Use root butler
        calib_date = date_to_ymd(calib_date)

        # Get dataIds for the raw calib frames
        keys = self.get_keys("raw")
        metalist = butler.queryMetadata("raw", format=keys, dataId={'dataType': calib_type})
        data_ids = [{k: v for k, v in zip(keys, m)} for m in metalist]

        self.logger.info(f"Found {len(data_ids)} dataIds to make master {calib_type} frames with.")

        # Construct the master calibs
        self.logger.debug(f"Creating master {calib_type} frames for calibDate={calib_date} with"
                          f" dataIds: {data_ids}.")
        tasks.make_master_calibs(calib_type, data_ids, butler=butler, rerun=rerun,
                                 calib_date=calib_date, butler_directory=self.butler_directory,
                                 calib_directory=self.calib_directory, **kwargs)

        # Get filenames of the master calibs
        calib_dir = os.path.join(self.butler_directory, "rerun", rerun)
        _, filenames = utils.get_files_of_type(f"calibrations.{calib_type}", directory=calib_dir,
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
        self._refcat_filename = os.path.join(self.butler_directory, "refcat_raw", "refcat_raw.csv")
        self._initialise()
        return self

    def __exit__(self, *args, **kwargs):
        """Close temporary directory."""
        self._butlers = {}
        self._tempdir.cleanup()
        self.butler_directory = None
        self._refcat_filename = None

    @property
    def calib_directory(self):
        if self.butler_directory is None:
            return None
        return os.path.join(self.butler_directory, "CALIB")
