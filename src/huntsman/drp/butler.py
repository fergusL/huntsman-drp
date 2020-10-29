import os
from contextlib import suppress
from collections import defaultdict
from tempfile import TemporaryDirectory

import sqlite3
import lsst.daf.persistence as dafPersist

import huntsman.drp.lsst_tasks as lsst
from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import date_to_ymd


class ButlerRepository(HuntsmanBase):
    _mapper = "lsst.obs.huntsman.HuntsmanMapper"

    def __init__(self, directory, calib_directory=None, initialise=True, **kwargs):
        super().__init__(**kwargs)
        self.butler_directory = directory
        if (calib_directory is None) and (directory is not None):
            calib_directory = os.path.join(directory, "CALIB")
        self._calib_directory = calib_directory
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

    def make_master_calibs(self, calib_date, rerun, skip_bias=False, **kwargs):
        """
        Make master calibs from ingested raw calibs.

        Args:
            calib_date (object): The calib date to assign to the master calibs.
            rerun (str): The name of the rerun.
            skip_bias (bool, optional): Skip creation of master biases? Default False.
        """
        if not skip_bias:
            self.make_master_biases(calib_date, rerun, **kwargs)
        self.make_master_flats(calib_date, rerun, **kwargs)

    def make_master_biases(self, calib_date, rerun, nodes=1, procs=1, ingest=True):
        """

        """
        metalist = self.butler.queryMetadata('raw', ['ccd', 'expTime', 'dateObs', 'visit'],
                                             dataId={'dataType': 'bias'})
        # Select the exposures we are interested in
        exposures = defaultdict(dict)
        for (ccd, exptime, dateobs, visit) in metalist:
            if exptime not in exposures[ccd].keys():
                exposures[ccd][exptime] = []
            exposures[ccd][exptime].append(visit)

        # Parse the calib date
        calib_date = date_to_ymd(calib_date)

        # Construct the calib for this ccd/exptime combination (do we need this split?)
        for ccd, exptimes in exposures.items():
            for exptime, data_ids in exptimes.items():
                self.logger.debug(f'Making master biases for ccd {ccd} using {len(data_ids)}'
                                  f' exposures of {exptime}s.')
                lsst.constructBias(butler_directory=self.butler_directory, rerun=rerun,
                                   calib_directory=self.calib_directory, data_ids=data_ids,
                                   exptime=exptime, ccd=ccd, nodes=nodes, procs=procs,
                                   calib_date=calib_date)
        if ingest:
            self.ingest_master_biases(calib_date, rerun=rerun)

    def make_master_flats(self, calib_date, rerun, nodes=1, procs=1, ingest=True):
        """

        """
        metalist = self.butler.queryMetadata('raw', ['ccd', 'filter', 'dateObs', 'visit'],
                                             dataId={'dataType': 'flat'})

        # Select the exposures we are interested in
        exposures = defaultdict(dict)
        for (ccd, filter_name, dateobs, visit) in metalist:
            if filter_name not in exposures[ccd].keys():
                exposures[ccd][filter_name] = []
            exposures[ccd][filter_name].append(visit)

        # Parse the calib date
        calib_date = date_to_ymd(calib_date)

        # Construct the calib for this ccd/filter combination (do we need this split?)
        for ccd, filter_names in exposures.items():
            for filter_name, data_ids in filter_names.items():
                self.logger.debug(f'Making master flats for ccd {ccd} using {len(data_ids)}'
                                  f' exposures in {filter_name} filter.')
                lsst.constructFlat(butler_directory=self.butler_directory, rerun=rerun,
                                   calib_directory=self.calib_directory, data_ids=data_ids,
                                   filter_name=filter_name, ccd=ccd, nodes=nodes,
                                   procs=procs, calib_date=calib_date)
        if ingest:
            self.ingest_master_flats(calib_date, rerun=rerun)

    def ingest_master_biases(self, calib_date, rerun, validity=1000):
        """ """
        lsst.ingest_master_biases(calib_date, self.butler_directory, self.calib_directory, rerun,
                                  validity=validity)

    def ingest_master_flats(self, calib_date, rerun, validity=1000):
        """ """
        lsst.ingest_master_flats(calib_date, self.butler_directory, self.calib_directory, rerun,
                                 validity=validity)

    def make_calexps(self, filter_name, rerun):
        """Make calibrated science exposures (calexps) by running `processCcd.py`.

        Args:
            filter_name (str): The filter name.
            rerun (str): The rerun name.
        """
        self.logger.debug(f"Making calexps for {filter_name} filter.")
        lsst.processCcd(self.butler_directory, self.calib_directory,
                        rerun=rerun, filter_name=filter_name)

    def make_coadd(self, filter_names, rerun):
        """
        Produce a coadd image.
        Args:
            filter_names (iterable of str): Iterable of filter names to make coadds with.
            rerun (str): Name of rerun.
        """
        lsst.makeDiscreteSkyMap(butler_directory=self.butler_directory, rerun=f'{rerun}:coadd')
        for filter_name in filter_names:
            self.logger.debig(f"Making coadd in {filter_name} filter.")
            lsst.makeCoaddTempExp(filter_name, butler_directory=self.butler_directory,
                                  calib_directory=self.calib_directory, rerun=f'{rerun}:coadd')
            lsst.assembleCoadd(filter_name, butler_directory=self.butler_directory,
                               calib_directory=self.calib_directory, rerun=f'{rerun}:coadd')

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
