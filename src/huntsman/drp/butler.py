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
from huntsman.drp.lsst.utils.coadd import get_skymap_ids
from huntsman.drp.utils.calib import get_calib_filename


class ButlerRepository(HuntsmanBase):
    _mapper = "lsst.obs.huntsman.HuntsmanMapper"
    _policy_filename = Policy.defaultPolicyFile("obs_huntsman", "HuntsmanMapper.yaml",
                                                relativePath="policy")
    _ra_key = "RA-MNT"
    _dec_key = "DEC-MNT"  # TODO: Move to config

    def __init__(self, directory, calib_dir=None, initialise=True, **kwargs):
        super().__init__(**kwargs)

        if directory is not None:
            directory = os.path.abspath(directory)
        self.butler_dir = directory

        if (calib_dir is None) and (directory is not None):
            calib_dir = os.path.join(self.butler_dir, "CALIB")
        self._calib_dir = calib_dir

        self._calib_validity = self.config["calibs"]["validity"]

        if self.butler_dir is None:
            self._refcat_filename = None
        else:
            self._refcat_filename = os.path.join(self.butler_dir, "refcat_raw", "refcat_raw.csv")

        # Load the policy file
        self._policy = Policy(self._policy_filename)

        # Initialise the butler repository
        self._butlers = {}  # One butler for each rerun
        if initialise:
            self._initialise()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    @property
    def calib_dir(self):
        return self._calib_dir

    @property
    def status(self):
        # TODO: Information here about number of ingested files etc
        raise NotImplementedError

    # Getters

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
            self.logger.debug(f"Creating new butler object for rerun={rerun}.")
            if rerun is None:
                butler_dir = self.butler_dir
            else:
                butler_dir = os.path.join(self.butler_dir, "rerun", rerun)
            os.makedirs(butler_dir, exist_ok=True)
            self._butlers[rerun] = dafPersist.Butler(inputs=butler_dir)
        return self._butlers[rerun]

    def get(self, dataset_type, data_id=None, rerun=None, **kwargs):
        """ Get a dataset from the butler repository.
        Args:
            dataset_type (str): The dataset type (raw, flat, bias etc.).
            data_id (dict): The data ID that uniquely specifies a file.
            rerun (str, optional): The rerun name. If None (default), will use the root butler
                directory.
        Returns:
            object: The dataset.
        """
        butler = self.get_butler(rerun=rerun)
        return butler.get(dataset_type, dataId=data_id, **kwargs)

    def get_keys(self, dataset_type, **kwargs):
        """ Get set of keys required to uniquely identify ingested data.
        Args:
            dataset_type (str): The dataset type (raw, flat, bias etc.).
        Returns:
            list of str: A list of keys.
        """
        butler = self.get_butler(**kwargs)
        return list(butler.getKeys(dataset_type))

    def get_filename(self, dataset_type, data_id, **kwargs):
        """ Get the filename for a data ID of data type.
        Args:
            dataset_type (str): The dataset type (raw, flat, bias etc.).
            data_id (dict): The data ID that uniquely specifies a file.
        Returns:
            str: The filename.
        """
        return self.get(dataset_type + "_filename", data_id=data_id, **kwargs)

    def get_metadata(self, dataset_type, keys, data_id=None, **kwargs):
        """ Get metadata for a dataset.
        Args:
            dataset_type (str): The dataset type (e.g. raw, flat, calexp).
            keys (list of str): The keys contained in the metadata.
            data_id (optional): A list of dataIds to query on.
        """
        butler = self.get_butler(**kwargs)
        md = butler.queryMetadata(dataset_type, format=keys, dataId=data_id)

        if len(keys) == 1:  # Butler doesn't return a consistent data structure if len(keys)=1
            return [{keys[0]: _} for _ in md]

        return [{k: v for k, v in zip(keys, _)} for _ in md]

    def get_calib_metadata(self, dataset_type, keys_ignore=None):
        """ Query the ingested calibs. TODO: Replace with the "official" Butler version.
        Args:
            dataset_type (str): The dataset type (e.g. bias, dark, flat).
            keys_ignore (list of str, optional): If provided, drop these keys from result.
        Returns:
            list of dict: The query result in column: value.
        """
        # Access the sqlite DB
        conn = sqlite3.connect(os.path.join(self.calib_dir, "calibRegistry.sqlite3"))
        c = conn.cursor()

        # Query the calibs
        result = c.execute(f"SELECT * from {dataset_type}")
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

    def get_data_ids(self, dataset_type, data_id=None, extra_keys=None, **kwargs):
        """ Get ingested data_ids for a given dataset_type.
        Args:
            dataset_type (str): The dataset_type (raw, bias, flat etc.).
            data_id (dict, optional): A complete or partial data_id to match with.
            extra_keys (list, optional): List of additional keys to be included in the data_ids.
        Returns:
            list of dict: A list of data_ids.
        """
        butler = self.get_butler(**kwargs)

        keys = list(butler.getKeys(dataset_type).keys())
        if extra_keys is not None:
            keys.extend(extra_keys)

        return self.get_metadata(dataset_type, keys=keys, data_id=data_id)

    def get_calexp_data_ids(self, rerun="default", filter_name=None, **kwargs):
        """ Convenience function to get data_ids for calexps.
        Args:
            rerun (str, optional): The rerun name. Default: "default".
            filter_name (str, optional): If given, only return data Ids for this filter.
            **kwargs: Parsed to self.get_data_ids.
        Returns:
            list of dict: The list of dataIds.
        """
        data_id = {"dataType": "science"}
        if filter_name is not None:
            data_id["filter"] = filter_name

        return self.get_data_ids("calexp", data_id=data_id, rerun=rerun, **kwargs)

    def get_calexps(self, rerun="default", **kwargs):
        """ Convenience function to get the calexps produced in a given rerun.
        Args:
            rerun (str, optional): The rerun name. Default: "default".
            **kwargs: Parsed to self.get_calexp_data_ids.
        Returns:
            list of lsst.afw.image.exposure: The list of calexp objects.
        """
        data_ids = self.get_calexp_data_ids(rerun=rerun, **kwargs)

        calexps = [self.get("calexp", data_id=d, rerun=rerun) for d in data_ids]
        if len(calexps) != len(data_ids):
            raise RuntimeError("Number of data_ids does not match the number of calexps.")

        return calexps, data_ids

    # Ingesting

    def ingest_raw_data(self, filenames, **kwargs):
        """ Ingest raw data into the repository.
        Args:
            filenames (iterable of str): The list of raw data filenames.
        """
        self.logger.debug(f"Ingesting {len(filenames)} file(s).")
        tasks.ingest_raw_data(filenames, butler_dir=self.butler_dir, **kwargs)

    def ingest_reference_catalogue(self, filenames):
        """ Ingest the reference catalogue into the repository.
        Args:
            filenames (iterable of str): The list of filenames containing reference data.
        """
        self.logger.debug(f"Ingesting reference catalogue from {len(filenames)} file(s).")
        tasks.ingest_reference_catalogue(self.butler_dir, filenames)

    def ingest_master_calibs(self, calib_type, filenames, validity=None):
        """ Ingest the master calibs into the butler repository.
        Args:
            calib_type (str): The calib dataset type (e.g. bias, flat).
            filenames (list of str): The files to ingest.
            validity (int, optional): How many days the calibs remain valid for. Default 1000.
        """
        if validity is None:
            validity = self._calib_validity
        self.logger.info(f"Ingesting {len(filenames)} master {calib_type} calib(s) with validity="
                         f"{validity}.")
        tasks.ingest_master_calibs(calib_type, filenames, self.butler_dir, self.calib_dir,
                                   validity=validity)

    # Making

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
            self._verify_master_calibs(calib_type)

    def make_reference_catalogue(self, ingest=True, **kwargs):
        """ Make the reference catalogue for the ingested science frames.
        Args:
            ingest (bool, optional): If True (default), ingest refcat into butler repo.
        """
        butler = self.get_butler(**kwargs)

        # Get the filenames of ingested images
        data_ids, filenames = utils.get_files_of_type("exposures.raw", self.butler_dir,
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

    def make_calexps(self, rerun="default", **kwargs):
        """ Make calibrated exposures (calexps) using the LSST stack.
        Args:
            rerun (str, optional): The name of the rerun. Default is "default".
            procs (int, optional): Run on this many processes (default 1).
        """
        # Get data_ids for the raw science frames
        data_ids = self.get_data_ids(dataset_type="raw", data_id={'dataType': "science"},
                                     extra_keys=["filter"])

        self.logger.info(f"Making calexp(s) from {len(data_ids)} data_id(s).")

        # Process the science frames
        tasks.make_calexps(data_ids, rerun=rerun, butler_dir=self.butler_dir,
                           calib_dir=self.calib_dir, **kwargs)

        # Check if we have the right number of calexps
        if not len(self.get_calexps(rerun=rerun)[0]) == len(data_ids):
            raise RuntimeError("Number of calexps does not match the number of data_ids.")

    def make_coadd(self, filter_names=None, rerun="default:coadd", **kwargs):
        """ Make a coadd from all the calexps in this repository.
        See: https://pipelines.lsst.io/getting-started/coaddition.html
        Args:
            filter_names (list, optional): The list of filter names to process. If not given,
                all filters will be processed.
            rerun (str, optional): The rerun name. Default is "default:coadd".
        """
        # Make the skymap in a chained rerun
        self.logger.info(f"Creating sky map with rerun: {rerun}.")
        tasks.make_discrete_sky_map(self.butler_dir, calib_dir=self.calib_dir, rerun=rerun)

        # Get the output rerun
        rerun_out = rerun.split(":")[-1]

        # Get the tract / patch indices from the skymap
        skymap_ids = self._get_skymap_ids(rerun=rerun_out)

        # Process all filters if filter_names is not provided
        if filter_names is None:
            md = self.get_metadata("calexp", keys=["filter"], data_id={"dataType": "science"})
            filter_names = list(set([_["filter"] for _ in md]))

        self.logger.info(f"Creating coadd in {len(filter_names)} filter(s).")

        for filter_name in filter_names:
            for tract_id, patch_ids in skymap_ids.items():  # TODO: Use multiprocessing

                self.logger.debug(f"Warping calexps for tract {tract_id} in {filter_name} filter.")

                task_kwargs = dict(butler_dir=self.butler_dir, calib_dir=self.calib_dir,
                                   rerun=rerun_out, tract_id=tract_id,
                                   patch_ids=patch_ids, filter_name=filter_name)

                # Warp the calexps onto skymap
                tasks.make_coadd_temp_exp(**task_kwargs)

                # Combine the warped calexps
                tasks.assemble_coadd(**task_kwargs)

        # Check all tracts and patches exist in each filter
        self._verify_coadd(rerun=rerun_out, filter_names=filter_names)

        self.logger.info("Successfully created coadd.")

    # Archiving

    def archive_master_calibs(self):
        """ Copy the master calibs from this Butler repository into the calib archive directory
        and insert the metadata into the master calib metadatabase.
        """
        calib_datatable = MasterCalibTable(config=self.config, logger=self.logger)

        for calib_type in self.config["calibs"]["types"]:

            # Retrieve filenames and data_ids for all files of this type
            data_ids, filenames = utils.get_files_of_type(f"calibrations.{calib_type}",
                                                          directory=self.calib_dir,
                                                          policy=self._policy)
            for metadata, filename in zip(data_ids, filenames):

                metadata["datasetType"] = calib_type

                # Create the filename for the archived copy
                archived_filename = get_calib_filename(config=self.config, **metadata)
                metadata["filename"] = archived_filename

                # Copy the file into the calib archive
                self.logger.debug(f"Copying {filename} to {archived_filename}.")
                os.makedirs(os.path.dirname(archived_filename), exist_ok=True)
                shutil.copy(filename, archived_filename)

                # Insert the metadata into the calib database
                calib_datatable.insert_one(metadata, overwrite=True)

    # Private methods

    def _initialise(self):
        """Initialise a new butler repository."""
        # Add the mapper file to each subdirectory, making directory if necessary
        for subdir in ["", "CALIB"]:
            dir = os.path.join(self.butler_dir, subdir)
            with suppress(FileExistsError):
                os.mkdir(dir)
            filename_mapper = os.path.join(dir, "_mapper")
            with open(filename_mapper, "w") as f:
                f.write(self._mapper)

    def _verify_master_calibs(self, dataset_type, raise_error=True):
        """ Check that the correct number of master calibs have been created following a call
        to make_master_calibs. This function compares the set of calibIds that should be ingested
        (using ingested raw calibs) to the calibIds that actually exist and are ingested.
        Args:
            dataset_type (str): The dataset type. Should be a valid calib dataset type (e.g. bias).
            raise_error (bool, optional): If True (default), an error will be raised if there
                are missing calibIds. Else, a warning is generated.
        """
        butler = self.get_butler()  # Use root butler

        keys_ignore = ["id", "calibDate", "validStart", "validEnd"]

        extra_keys = []
        if dataset_type == "flat":
            extra_keys.append("filter")  # TODO: Get this info somewhere else

        # Get data_ids of raw ingested calibs
        raw_ids = self.get_data_ids(dataset_type="raw", data_id={'dataType': dataset_type},
                                    extra_keys=extra_keys)

        # Get calibIds of master calibs that *should* be ingested
        calib_ids_required = utils.data_id_to_calib_id(dataset_type, raw_ids, butler=butler,
                                                       keys_ignore=keys_ignore)

        # Get calibIds of ingested master calibs
        calib_ids_ingested = self.get_calib_metadata(dataset_type, keys_ignore=keys_ignore)

        # Check for missing master calibs
        missing_ids = utils.get_missing_data_ids(calib_ids_ingested, calib_ids_required)

        # Handle result
        if len(missing_ids) > 0:
            msg = f"{len(missing_ids)} missing master {dataset_type} calibs: {missing_ids}."
            if raise_error:
                self.logger.error(msg)
                raise FileNotFoundError(msg)
            else:
                self.logger.warning(msg)
        else:
            self.logger.debug(f"No missing {dataset_type} calibs detected.")

    def _make_master_calibs(self, calib_type, calib_date, rerun, ingest=True, validity=None,
                            **kwargs):
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

        # Get data_ids for the raw calib frames
        data_ids = self.get_data_ids("raw", data_id={'dataType': calib_type})
        self.logger.info(f"Found {len(data_ids)} data_id(s) to make master {calib_type}"
                         " frames with.")

        # Construct the master calibs
        self.logger.debug(f"Creating master {calib_type} frames for calib_date={calib_date} with"
                          f" data_ids: {data_ids}.")
        tasks.make_master_calibs(calib_type, data_ids, butler=butler, rerun=rerun,
                                 calib_date=calib_date, butler_dir=self.butler_dir,
                                 calib_dir=self.calib_dir, **kwargs)

        # Get filenames of the master calibs
        calib_dir = os.path.join(self.butler_dir, "rerun", rerun)
        _, filenames = utils.get_files_of_type(f"calibrations.{calib_type}", directory=calib_dir,
                                               policy=self._policy)

        # Ingest the masters into the butler repo
        if ingest:
            self.ingest_master_calibs(calib_type, filenames, validity=validity)

        return filenames

    def _get_skymap_ids(self, rerun):
        """ Get the sky map IDs, which consist of a tract ID and associated patch IDs.
        Args:
            rerun (str): The rerun name.
        Returns:
            dict: A dict of tract_id: [patch_ids].
        """
        skymap = self.get("deepCoadd_skyMap", rerun=rerun)
        return get_skymap_ids(skymap)

    def _verify_coadd(self, filter_names, rerun):
        """ Verify all the coadd patches exist and can be found by the Butler.
        Args:
            rerun (str): The rerun name.
            filter_names (list of str): The list of filter names to check.
        Raises:
            Exception: An unspecified exception is raised if there is a problem with the coadd.
        """
        self.logger.info("Verifying coadd.")

        butler = self.get_butler(rerun=rerun)
        skymap_ids = self._get_skymap_ids(rerun=rerun)

        for filter_name in filter_names:
            for tract_id, patch_ids in skymap_ids.items():
                for patch_id in patch_ids:

                    data_id = {"tract": tract_id, "patch": patch_id, "filter": filter_name}
                    try:
                        butler.get("deepCoadd", dataId=data_id)
                    except Exception as err:
                        self.logger.error(f"Error encountered while verifying coadd: {err!r}")
                        raise err


class TemporaryButlerRepository(ButlerRepository):
    """ Create a new Butler repository in a temporary directory."""

    def __init__(self, **kwargs):
        super().__init__(directory=None, initialise=False, **kwargs)

    def __enter__(self):
        """Create temporary directory and initialise as a butler repository."""
        self._tempdir = TemporaryDirectory()
        self.butler_dir = self._tempdir.name
        self._refcat_filename = os.path.join(self.butler_dir, "refcat_raw", "refcat_raw.csv")
        self._initialise()
        return self

    def __exit__(self, *args, **kwargs):
        """Close temporary directory."""
        self._butlers = {}
        self._tempdir.cleanup()
        self.butler_dir = None
        self._refcat_filename = None

    @property
    def calib_dir(self):
        if self.butler_dir is None:
            return None
        return os.path.join(self.butler_dir, "CALIB")
