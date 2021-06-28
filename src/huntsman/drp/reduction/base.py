import os
from collections import defaultdict

import pandas as pd
import matplotlib.pyplot as plt

from huntsman.drp.utils import normalise_path, plotting
from huntsman.drp.base import HuntsmanBase
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection
from huntsman.drp.refcat import RefcatClient


class ReductionBase(HuntsmanBase):
    """ Generic class for data reductions. """

    def __init__(self, name, query, directory=None, exposure_collection=None, calib_collection=None,
                 initialise=True, nproc=1, **kwargs):

        super().__init__(**kwargs)

        self.nproc = int(nproc)

        # Specify directory for reduction
        if directory:
            directory = normalise_path(directory)
        else:
            directory = self.config["directories"]["reductions"]
        self.directory = os.path.join(directory, name)

        self._query = query

        self._refcat_filename = os.path.join(self.directory, "refcat.csv")

        if not exposure_collection:
            exposure_collection = RawExposureCollection(config=self.config)
        self._exposure_collection = exposure_collection

        if not calib_collection:
            calib_collection = MasterCalibCollection(config=self.config)
        self._calib_collection = calib_collection

        self.science_docs = self._exposure_collection.find(**self._query)
        self.calib_docs = self._get_calibs(self.science_docs)

        if initialise:
            self._initialise()

    # Properties

    # Methods

    def prepare(self):
        """ Prepare the data to reduce.
        This method is responsible for querying the database, ingesting the files and producing
        the reference catalogue.
        """
        # Make reference catalogue
        self._make_reference_catalogue()

    def reduce(self):
        """ Make coadds and store results in the reduction directory.
        """
        raise NotImplementedError

    def run(self, makeplots=True, **kwargs):
        """ Convenience method to run the whole thing. """
        self.prepare()

        if makeplots:
            self.make_prepare_plots()

        self.reduce(**kwargs)

        if makeplots:
            self.make_reduce_plots()

    # Plotting methods

    def make_prepare_plots(self, dpi=150):
        """ Make summary plots after the preparation stage.
        Args:
            dpi (int): Dots per inch for the saved figure.
        """
        fig, ax = plotting.plot_wcs_boxes(self.science_docs)

        ra_key = self.config["refcat"]["ra_key"]
        dec_key = self.config["refcat"]["dec_key"]

        df = pd.read_csv(self._refcat_filename)
        ax.plot(df[ra_key].values, df[dec_key].values, "bo", markersize=1)

        plt.savefig(os.path.join(self.directory, "plots", "refobjs.png"), bbox_inches="tight",
                    dpi=dpi)

    def make_reduce_plots(self):
        """ Make summary plots after the reduction stage. """
        pass

    # Private methods

    def _initialise(self):
        """ Abstract instance method responsible for initialising the data reduction.
        """
        os.makedirs(self.directory, exist_ok=True)
        os.makedirs(os.path.join(self.directory, "plots"), exist_ok=True)

    def _get_calibs(self, science_docs):
        """ Get matching calib docs for a set of science docs.
        Args:
            science_docs (list of RawExposureDocument): The list of science docs to match with.
        Returns:
            dict of set: Dictionary of calib type: set of matching calib documents.
        """
        calib_docs = defaultdict(list)

        for doc in science_docs:
            calibs = self._calib_collection.get_matching_calibs(doc)

            for k, v in calibs.items():
                calib_docs[k].append(v)

        return {k: set(v) for k, v in calib_docs.items()}

    def _make_reference_catalogue(self):
        """ Make reference catalogue and write it to file in reduction directory. """

        refcat = RefcatClient(config=self.config)

        refcat.make_from_documents(self.science_docs, filename=self._refcat_filename)
