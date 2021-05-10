import os

from huntsman.drp.reduction.base import DataReductionBase
from huntsman.drp.lsst.butler import ButlerRepository


class LsstDataReduction(DataReductionBase):

    """ Data reduction using LSST stack. """

    def __init__(self, calexp_kwargs=None, coadd_kwargs=None, *args, **kwargs):
        super().__init__(initialise=False, *args, **kwargs)

        self._butler_directory = os.path.join(self.directory, "lsst")
        self._butler_repo = None

        # Setup task configs

        self._calexp_kwargs = {} if calexp_kwargs is None else calexp_kwargs
        self._calexp_kwargs["procs"] = self.nproc

        self._coadd_kwargs = {} if coadd_kwargs is None else coadd_kwargs

        self._initialise()

    # Methods

    def prepare(self):
        """ Override the prepare method to ingest the files into the butler repository. """
        super().prepare()

        # Ingest raw files into butler repository
        self._butler_repo.ingest_raw_data([d["filename"] for d in self.science_docs])

        # Ingest master calibs into butler repository
        for datasetType, docs in self.calib_docs.items():
            self._butler_repo.ingest_master_calibs(datasetType, [d["filename"] for d in docs])

        # Ingest reference catalogue
        self._butler_repo.ingest_reference_catalogue([self._refcat_filename])

    def reduce(self):
        """ Use the LSST stack to calibrate and stack exposures. """

        self._butler_repo.make_calexps(**self._calexp_kwargs)

        self._butler_repo.make_coadd(**self._coadd_kwargs)

    # Private methods

    def _initialise(self):
        """ Override method to create the butler repository. """
        super()._initialise()

        # Initialise the butler repository
        self._butler_repo = ButlerRepository(self._butler_directory, config=self.config)
