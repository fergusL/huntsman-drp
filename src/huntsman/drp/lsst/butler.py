"""
NOTES:
 -  The correct way to create a Butler instance:
    https://github.com/lsst/pipe_base/blob/master/python/lsst/pipe/base/argumentParser.py#L678
"""
import os
import sqlite3
from contextlib import suppress
from tempfile import TemporaryDirectory

import lsst.daf.persistence as dafPersist

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.lsst import tasks
import huntsman.drp.lsst.utils.butler as utils
from huntsman.drp.lsst.utils.coadd import get_skymap_ids
from huntsman.drp.lsst.utils.calib import get_calib_filename


class ButlerRepository(HuntsmanBase):

    _mapper = "lsst.obs.huntsman.HuntsmanMapper"

    def __init__(self, directory, calib_dir=None, initialise=True, calib_validity=1000, **kwargs):
        """
        Args:
            directory (str): The path of the butler reposity.
            calib_dir (str, optional): The path of the butler calib repository. If None (default),
                will create a new CALIB directory under the butler repository root.
            initialise (bool, optional): If True (default), initialise the butler reposity
                with required files.
        """
        super().__init__(**kwargs)

        self._ordered_calib_types = self.config["calibs"]["types"]

        if directory is not None:
            directory = os.path.abspath(directory)
        self.butler_dir = directory

        if (calib_dir is None) and (directory is not None):
            calib_dir = os.path.join(self.butler_dir, "CALIB")
        self._calib_dir = calib_dir

        self._calib_validity = calib_validity

        if self.butler_dir is None:
            self._refcat_filename = None
        else:
            self._refcat_filename = os.path.join(self.butler_dir, "refcat_raw", "refcat_raw.csv")

        # Load the policy file
        self._policy = utils.load_policy()

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

    def document_to_dataId(self, document):
        """ Extract an LSST dataId from a RawExposureDocument.
        Args:
            document (RawExposureDocument): The document to convert.
        Returns:
            dict: The corresponding dataId.
        """
        return {k: document[k] for k in self.get_keys("raw")}

    def get_butler(self, rerun=None):
        """ Get a butler object for a given rerun.
        We cache created butlers to avoid the overhead of having to re-create them each time.
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

            inputs = {"root": butler_dir}
            outputs = {'root': butler_dir, 'mode': 'rw'}

            if rerun:
                outputs["cfgRoot"] = self.butler_dir

            butler_kwargs = {"mapperArgs": {"calibRoot": self._calib_dir}}
            inputs.update(butler_kwargs)
            outputs.update(butler_kwargs)

            self._butlers[rerun] = dafPersist.Butler(inputs=inputs, outputs=outputs)

        return self._butlers[rerun]

    def get(self, datasetType, dataId=None, rerun=None, **kwargs):
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
        return self.get(datasetType + "_filename", dataId=dataId, **kwargs)[0]

    def get_metadata(self, datasetType, keys=None, dataId=None, **kwargs):
        """ Get metadata for a dataset.
        Args:
            datasetType (str): The dataset type (e.g. raw, flat, calexp).
            keys (list of str, optional): The keys contained in the metadata. If not provided,
                will use default keys for datasetType.
            dataId (optional): A list of dataIds to query on.
        """
        if keys is None:
            keys = self.get_keys(datasetType, **kwargs)

        butler = self.get_butler(**kwargs)
        md = butler.queryMetadata(datasetType, format=keys, dataId=dataId)

        if len(keys) == 1:  # Butler doesn't return a consistent data structure if len(keys)=1
            return [{keys[0]: _} for _ in md]

        return [{k: v for k, v in zip(keys, _)} for _ in md]

    def get_dataIds(self, datasetType, dataId=None, extra_keys=None, **kwargs):
        """ Get ingested dataIds for a given datasetType.
        Args:
            datasetType (str): The datasetType (raw, bias, flat etc.).
            dataId (dict, optional): A complete or partial dataId to match with.
            extra_keys (list, optional): List of additional keys to be included in the dataIds.
        Returns:
            list of dict: A list of dataIds.
        """
        butler = self.get_butler(**kwargs)

        keys = list(butler.getKeys(datasetType).keys())
        if extra_keys is not None:
            keys.extend(extra_keys)

        # Work-around to have consistent query behaviour for calibs
        # TODO: Figure out how to do this properly with butler
        if datasetType in self._ordered_calib_types:
            results = []
            for md in self._get_calib_metadata(datasetType):
                if dataId is not None:
                    if not all([k in md for k in dataId.keys()]):
                        continue
                    elif not all(md[k] == dataId[k] for k in dataId.keys()):
                        continue
                results.append({k: md[k] for k in keys})
            return results

        return self.get_metadata(datasetType, keys=keys, dataId=dataId)

    def get_calexp_dataIds(self, rerun="default", filter_name=None, **kwargs):
        """ Convenience function to get dataIds for calexps.
        Args:
            rerun (str, optional): The rerun name. Default: "default".
            filter_name (str, optional): If given, only return data Ids for this filter.
            **kwargs: Parsed to self.get_dataIds.
        Returns:
            list of dict: The list of dataIds.
        """
        dataId = {"dataType": "science"}
        if filter_name is not None:
            dataId["filter"] = filter_name

        return self.get_dataIds("calexp", dataId=dataId, rerun=rerun, **kwargs)

    def get_calexps(self, dataIds=None, rerun="default", **kwargs):
        """ Convenience function to get calexp objects for a given rerun.
        Args:
            dataIds (list, optional): If provided, get calexps for these dataIds only.
            rerun (str, optional): The rerun name. Default: "default".
            **kwargs: Parsed to self.get_calexp_dataIds.
        Returns:
            list of lsst.afw.image.exposure: The list of calexp objects.
        """
        if dataIds is None:
            dataIds = self.get_calexp_dataIds(rerun=rerun, **kwargs)

        calexps = [self.get("calexp", dataId=d, rerun=rerun) for d in dataIds]
        if len(calexps) != len(dataIds):
            raise RuntimeError("Number of dataIds does not match the number of calexps.")

        return calexps, dataIds

    def ingest_raw_data(self, filenames, **kwargs):
        """ Ingest raw data into the repository.
        Args:
            filenames (iterable of str): The list of raw data filenames.
        """
        filenames = set([os.path.abspath(os.path.realpath(_)) for _ in filenames])

        self.logger.debug(f"Ingesting {len(filenames)} file(s).")

        tasks.ingest_raw_data(filenames, butler_dir=self.butler_dir, **kwargs)

    def ingest_reference_catalogue(self, filenames):
        """ Ingest the reference catalogue into the repository.
        Args:
            filenames (iterable of str): The list of filenames containing reference data.
        """
        self.logger.debug(f"Ingesting reference catalogue from {len(filenames)} file(s).")
        tasks.ingest_reference_catalogue(self.butler_dir, filenames)

    def ingest_master_calibs(self, datasetType, filenames, validity=None):
        """ Ingest the master calibs into the butler repository.
        Args:
            datasetType (str): The calib dataset type (e.g. bias, flat).
            filenames (list of str): The files to ingest.
            validity (int, optional): How many days the calibs remain valid for. Default 1000.
        """
        filenames = set([os.path.abspath(os.path.realpath(_)) for _ in filenames])

        if not filenames:
            self.logger.warning(f"No master {datasetType} files to ingest.")
            return

        if validity is None:
            validity = self._calib_validity

        self.logger.info(f"Ingesting {len(filenames)} master {datasetType} calib(s) with validity="
                         f"{validity}.")
        tasks.ingest_master_calibs(datasetType, filenames, butler_dir=self.butler_dir,
                                   calib_dir=self.calib_dir, validity=validity)

    def make_master_calib(self, calib_doc, rerun="default", validity=None, **kwargs):
        """ Make a master calib from ingested raw exposures.
        Args:
            datasetType (str): The calib datasetType (e.g. bias, dark, flat).
            calib_doc (CalibDocument): The calib document of the calib to make.
            rerun (str, optional): The name of the rerun. Default is "default".
            validity (int, optional): The calib validity in days.
            **kwargs: Parsed to tasks.make_master_calib.
        Returns:
            str: The filename of the newly created master calib.
        """
        datasetType = calib_doc["datasetType"]
        calibId = self._calib_doc_to_calibId(calib_doc)

        # Get dataIds applicable to this calibId
        dataIds = self.calibId_to_dataIds(datasetType, calibId, with_calib_date=True)

        self.logger.info(f"Making master calib for calibId={calibId} from {len(dataIds)} dataIds.")

        # Make the master calib
        tasks.make_master_calib(datasetType, calibId, dataIds, butler_dir=self.butler_dir,
                                calib_dir=self.calib_dir, rerun=rerun, **kwargs)

        directory = os.path.join(self.butler_dir, "rerun", rerun)
        filename = get_calib_filename(calib_doc, directory=directory, config=self.config)

        # Check the calib exists
        if not os.path.isfile(filename):
            raise FileNotFoundError(f"Master calib not found: {calibId}, filename={filename}")

        # Ingest the calib
        self.ingest_master_calibs(datasetType, [filename], validity=validity)

        return filename

    def make_master_calibs(self, calib_docs, **kwargs):
        """ Make master calibs for a list of calib documents.
        Args:
            calib_docs (list of CalibDocument): The list of calib documents to make.
            **kwargs: Parsed to tasks.make_master_calib.
        Returns:
            dict: Dictionay containing lists of filename for each datasetType.
        """
        docs = []
        for datasetType in self._ordered_calib_types:  # Order is important

            for calib_doc in [c for c in calib_docs if c["datasetType"] == datasetType]:
                try:
                    filename = self.make_master_calib(calib_doc, **kwargs)

                    # Update the filename
                    doc = calib_doc.copy()
                    doc["filename"] = filename
                    docs.append(doc)

                except Exception as err:
                    self.logger.error(f"Problem making calib for calibId={calib_doc}: {err!r}")

        return docs

    def make_calexp(self, dataId, rerun="default", **kwargs):
        """ Make calibrated exposure using the LSST stack.
        Args:
            rerun (str, optional): The name of the rerun. Default is "default".
        """
        self.logger.info(f"Making calexp for {dataId}.")

        return tasks.make_calexp(dataId, rerun=rerun, butler_dir=self.butler_dir,
                                 calib_dir=self.calib_dir, **kwargs)

    def make_calexps(self, dataIds=None, rerun="default", **kwargs):
        """ Make calibrated exposures (calexps) using the LSST stack.
        Args:
            dataIds (list of dict): List of dataIds to process. If None (default), will process
                all ingested science exposures.
            rerun (str, optional): The name of the rerun. Default is "default".
            **kwargs: Parsed to `tasks.make_calexps`.
        """
        # Get dataIds for the raw science frames
        # TODO: Remove extra keys as this should be taken care of by policy now
        if dataIds is None:
            dataIds = self.get_dataIds(datasetType="raw", dataId={'dataType': "science"},
                                       extra_keys=["filter"])

        self.logger.info(f"Making calexp(s) from {len(dataIds)} dataId(s).")

        # Process the science frames in parallel using LSST taskRunner
        tasks.make_calexps(dataIds, rerun=rerun, butler_dir=self.butler_dir,
                           calib_dir=self.calib_dir, doReturnResults=False, **kwargs)

        # Check if we have the right number of calexps
        if not len(self.get_calexps(rerun=rerun, dataIds=dataIds)[0]) == len(dataIds):
            raise RuntimeError("Number of calexps does not match the number of dataIds.")

        self.logger.debug("Finished making calexps.")

    def make_coadd(self, dataIds=None, filter_names=None, rerun="default:coadd", **kwargs):
        """ Make a coadd from all the calexps in this repository.
        See: https://pipelines.lsst.io/getting-started/coaddition.html
        Args:
            filter_names (list, optional): The list of filter names to process. If not given,
                all filters will be independently processed.
            rerun (str, optional): The rerun name. Default is "default:coadd".
            dataIds (list, optional): The list of dataIds to process. If None (default), all files
                will be processed.
        """
        if dataIds is None:
            dataIds = self.get_dataIds("raw")

        # Make the skymap in a chained rerun
        # The skymap is a discretisation of the sky and defines the shapes and sizes of coadd tiles
        self.logger.info(f"Creating sky map with rerun: {rerun}.")
        tasks.make_discrete_sky_map(self.butler_dir, calib_dir=self.calib_dir, rerun=rerun,
                                    dataIds=dataIds)

        # Get the output rerun
        rerun_out = rerun.split(":")[-1]

        # Get the tract / patch indices from the skymap
        # A skymap ID consists of a tractId and associated patchIds
        skymapIds = self._get_skymap_ids(rerun=rerun_out)

        # Process all filters if filter_names is not provided
        if filter_names is None:
            md = self.get_metadata("calexp", keys=["filter"], dataId={"dataType": "science"})
            filter_names = list(set([_["filter"] for _ in md]))

        self.logger.info(f"Creating coadd in {len(filter_names)} filter(s).")

        for filter_name in filter_names:

            self.logger.info(f"Creating coadd in {filter_name} filter from"
                             f" {len(skymapIds)} tracts.")

            dataIds_filter = [d for d in dataIds if d["filter"] == filter_name]

            task_kwargs = dict(butler_dir=self.butler_dir, calib_dir=self.calib_dir,
                               rerun=rerun_out, skymapIds=skymapIds, dataIds=dataIds_filter,
                               filter_name=filter_name)

            # Warp the calexps onto skymap
            tasks.make_coadd_temp_exp(**task_kwargs)

            # Combine the warped calexps
            tasks.assemble_coadd(**task_kwargs)

        # Check all tracts and patches exist in each filter
        self._verify_coadd(rerun=rerun_out, filter_names=filter_names, skymapIds=skymapIds)

        self.logger.info("Successfully created coadd.")

    def calibId_to_dataIds(self, datasetType, calibId, limit=False, with_calib_date=False):
        """ Find all matching dataIds given a calibId.
        Args:
            calibId (dict): The calibId.
            limit (bool): If True, limit the number of returned dataIds to a maximum value
                indicated by self._max_dataIds_per_calib. This avoids long processing times and
                apparently also segfaults. Default: False.
        Returns:
            list of dict: All matching dataIds.
        """
        dataIds = utils.calibId_to_dataIds(datasetType, calibId, butler=self.get_butler())

        if with_calib_date:
            for dataId in dataIds:
                dataId["calibDate"] = calibId["calibDate"]

        return dataIds

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

    def _get_skymap_ids(self, rerun):
        """ Get the sky map IDs, which consist of a tract ID and associated patch IDs.
        Args:
            rerun (str): The rerun name.
        Returns:
            dict: A dict of tractId: [patchIds].
        """
        skymap = self.get("deepCoadd_skyMap", rerun=rerun)
        return get_skymap_ids(skymap)

    def _verify_coadd(self, skymapIds, filter_names, rerun):
        """ Verify all the coadd patches exist and can be found by the Butler.
        Args:
            rerun (str): The rerun name.
            filter_names (list of str): The list of filter names to check.
        Raises:
            Exception: An unspecified exception is raised if there is a problem with the coadd.
        """
        self.logger.info("Verifying coadd.")

        butler = self.get_butler(rerun=rerun)

        for filter_name in filter_names:
            for skymapId in skymapIds:

                tractId = skymapId["tractId"]
                patchIds = skymapId["patchIds"]

                for patchId in patchIds:
                    dataId = {"tract": tractId, "patch": patchId, "filter": filter_name}
                    try:
                        butler.get("deepCoadd", dataId=dataId)
                    except Exception as err:
                        self.logger.error(f"Error encountered while verifying coadd: {err!r}")
                        raise err

    def _calib_doc_to_calibId(self, calib_doc, **kwargs):
        """ Convert a CalibDocument into a LSST-style calibId.
        Args:
            calib_doc (CalibDocument): The calib document.
        Returns:
            dict: The calibId.
        """
        datasetType = calib_doc["datasetType"]
        required_keys = self.get_keys(datasetType, **kwargs)
        return {k: calib_doc[k] for k in required_keys}

    def _get_calib_metadata(self, datasetType, keys_ignore=None):
        """ Query the ingested calibs.
        TODO: Figure out how to do this properly with Butler.
        Args:
            datasetType (str): The dataset type (e.g. bias, dark, flat).
            keys_ignore (list of str, optional): If provided, drop these keys from result.
        Returns:
            list of dict: The query result in column: value.
        """
        # Access the sqlite DB
        conn = sqlite3.connect(os.path.join(self.calib_dir, "calibRegistry.sqlite3"))
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


class TemporaryButlerRepository(ButlerRepository):
    """ Create a new Butler repository in a temporary directory."""

    def __init__(self, directory_prefix=None, **kwargs):
        """
        Args:
            directory_prefix (str): String to prefix the name of the temporary directory.
                Default: None.
            **kwargs: Parsed to ButlerRepository init function.
        """
        self._directory_prefix = directory_prefix
        super().__init__(directory=None, initialise=False, **kwargs)

    def __enter__(self):
        """Create temporary directory and initialise as a butler repository."""
        self._tempdir = TemporaryDirectory(prefix=self._directory_prefix)
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
