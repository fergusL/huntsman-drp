""" Minimal overrides to remove unnecessary and time consuming file lock creation in current
implementation of ingestIndexReferenceTask."""

import multiprocessing

import astropy.units as u
import numpy as np

from lsst.meas.algorithms import IngestIndexedReferenceTask
from lsst.meas.algorithms.ingestIndexManager import IngestIndexManager


class singleProccessIngestIndexManager(IngestIndexManager):
    """
    Ingest a reference catalog from external files into a butler repository,
    using a multiprocessing Pool to speed up the work.
    Parameters
    ----------
    filenames : `dict` [`int`, `str`]
        The HTM pixel id and filenames to ingest the catalog into.
    config : `lsst.meas.algorithms.IngestIndexedReferenceConfig`
        The Task configuration holding the field names.
    file_reader : `lsst.pipe.base.Task`
        The file reader to use to load the files.
    indexer : `lsst.meas.algorithms.HtmIndexer`
        The class used to compute the HTM pixel per coordinate.
    schema : `lsst.afw.table.Schema`
        The schema of the output catalog.
    key_map : `dict` [`str`, `lsst.afw.table.Key`]
        The mapping from output field names to keys in the Schema.
    htmRange : `tuple` [`int`]
        The start and end HTM pixel ids.
    addRefCatMetadata : callable
        A function called to add extra metadata to each output Catalog.
    log : `lsst.log.Log`
        The log to send messages to.
    """
    _flags = ['photometric', 'resolved', 'variable']

    def __init__(self, filenames, config, file_reader, indexer,
                 schema, key_map, htmRange, addRefCatMetadata, log):
        self.filenames = filenames
        self.config = config
        self.file_reader = file_reader
        self.indexer = indexer
        self.schema = schema
        self.key_map = key_map
        self.htmRange = htmRange
        self.addRefCatMetadata = addRefCatMetadata
        self.log = log
        if self.config.coord_err_unit is not None:
            # cache this to speed up coordinate conversions
            self.coord_err_unit = u.Unit(self.config.coord_err_unit)
        self._couter = 0

    def run(self, inputFiles):
        """Index a set of input files from a reference catalog, and write the
        output to the appropriate filenames, in parallel.
        Parameters
        ----------
        inputFiles : `list`
            A list of file paths to read data from.
        """
        self.nInputFiles = len(inputFiles)

        with multiprocessing.Manager():
            self._counter = 0
            self._file_progress = 0
            for filename in inputFiles:
                self._ingestOneFile(filename)

    def _ingestOneFile(self, filename):
        """Read and process one file, and write its records to the correct
        indexed files.
        Parameters
        ----------
        filename : `str`
            The file to process.
        """
        inputData = self.file_reader.run(filename)
        fluxes = self._getFluxes(inputData)
        coordErr = self._getCoordErr(inputData)
        matchedPixels = self.indexer.indexPoints(inputData[self.config.ra_name],
                                                 inputData[self.config.dec_name])
        pixel_ids = set(matchedPixels)
        for pixelId in pixel_ids:
            self._doOnePixel(inputData, matchedPixels, pixelId, fluxes, coordErr)

        oldPercent = 100 * self._file_progress / self.nInputFiles
        self._file_progress += 1
        percent = 100 * self._file_progress / self.nInputFiles
        # only log each "new percent"
        if np.floor(percent) - np.floor(oldPercent) >= 1:
            self.log.info("Completed %d / %d files: %d %% complete ",
                          self._file_progress,
                          self.nInputFiles,
                          percent)

    def _doOnePixel(self, inputData, matchedPixels, pixelId, fluxes, coordErr):
        """Process one HTM pixel, appending to an existing catalog or creating
        a new catalog, as needed.
        Parameters
        ----------
        inputData : `numpy.ndarray`
            The data from one input file.
        matchedPixels : `numpy.ndarray`
            The row-matched pixel indexes corresponding to ``inputData``.
        pixelId : `int`
            The pixel index we are currently processing.
        fluxes : `dict` [`str`, `numpy.ndarray`]
            The values that will go into the flux and fluxErr fields in the
            output catalog.
        coordErr : `dict` [`str`, `numpy.ndarray`]
            The values that will go into the coord_raErr, coord_decErr, and
            coord_ra_dec_Cov fields in the output catalog (in radians).
        """
        idx = np.where(matchedPixels == pixelId)[0]
        catalog = self.getCatalog(pixelId, self.schema, len(idx))
        for outputRow, inputRow in zip(catalog[-len(idx):], inputData[idx]):
            self._fillRecord(outputRow, inputRow)

        self._setIds(inputData[idx], catalog)

        # set fluxes from the pre-computed array
        for name, array in fluxes.items():
            catalog[self.key_map[name]][-len(idx):] = array[idx]

        # set coordinate errors from the pre-computed array
        for name, array in coordErr.items():
            catalog[name][-len(idx):] = array[idx]

        catalog.writeFits(self.filenames[pixelId])

    def _setIds(self, inputData, catalog):
        """Fill the `id` field of catalog with a running index, filling the
        last values up to the length of ``inputData``.
        Fill with `self.config.id_name` if specified, otherwise use the
        global running counter value.
        Parameters
        ----------
        inputData : `numpy.ndarray`
            The input data that is being processed.
        catalog : `lsst.afw.table.SimpleCatalog`
            The output catalog to fill the ids.
        """
        size = len(inputData)
        if self.config.id_name:
            catalog['id'][-size:] = inputData[self.config.id_name]
        else:
            idEnd = self._counter + size
            catalog['id'][-size:] = np.arange(self._counter, idEnd)
            self._counter = idEnd


class HuntsmanIngestIndexedReferenceTask(IngestIndexedReferenceTask):
    """Class for producing and loading indexed reference catalogs.
    This implements an indexing scheme based on hierarchical triangular
    mesh (HTM). The term index really means breaking the catalog into
    localized chunks called shards.  In this case each shard contains
    the entries from the catalog in a single HTM trixel
    For producing catalogs this task makes the following assumptions
    about the input catalogs:
    - RA, Dec are in decimal degrees.
    - Epoch is available in a column, in a format supported by astropy.time.Time.
    - There are no off-diagonal covariance terms, such as covariance
      between RA and Dec, or between PM RA and PM Dec. Support for such
     covariance would have to be added to to the config, including consideration
     of the units in the input catalog.
    Parameters
    ----------
    butler : `lsst.daf.persistence.Butler`
        Data butler for reading and writing catalogs
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.IngestManager = singleProccessIngestIndexManager
