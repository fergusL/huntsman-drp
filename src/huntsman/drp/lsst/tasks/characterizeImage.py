""" Huntsman overrides for CharacterizeImageTask.

Changes:
- Adds try except statement around PSF measurement. This allows the source catalogue to be written
  in case it fails, which is useful for finding out why.
- Implements offset sky functionality.
"""
import numpy as np

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
from lsst.obs.base import ExposureIdInfo
from lsst.afw.math import BackgroundList
from lsst.afw.table import SourceTable, IdFactory
from lsst.pex.exceptions import LengthError
from lsst.pipe.tasks.characterizeImage import CharacterizeImageTask, CharacterizeImageConfig


class HuntsmanCharacterizeImageConfig(CharacterizeImageConfig):
    """ Override task config to add offset sky functionality. """

    useOffsetSky = pexConfig.Field(dtype=bool,
                                   default=False,
                                   doc="Use offset sky for initial sky estiamte")


class HuntsmanCharacterizeImageTask(CharacterizeImageTask):

    ConfigClass = HuntsmanCharacterizeImageConfig  # Set as default config class

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @pipeBase.timeMethod
    def runDataRef(self, dataRef, exposure=None, background=None, doUnpersist=True):
        """
        Huntsman overrides:
          - Get offset sky from butler and parse it to run method if config.useOffsetSky.
        """
        self._frame = self._initialFrame  # reset debug display frame
        self.log.info("Processing %s" % (dataRef.dataId))

        if doUnpersist:
            if exposure is not None or background is not None:
                raise RuntimeError("doUnpersist true; exposure and background must be None")
            exposure = dataRef.get("postISRCCD", immediate=True)
        elif exposure is None:
            raise RuntimeError("doUnpersist false; exposure must be provided")

        exposureIdInfo = dataRef.get("expIdInfo")

        # This is a Huntsman modification
        # Use the butler to obtain a ready-made offset sky background image
        if self.config.useOffsetSky:
            offset_sky_background = dataRef.get("offsetBackground")
        else:
            offset_sky_background = None

        # Parse the offset sky background to the run method
        charRes = self.run(exposure=exposure, exposureIdInfo=exposureIdInfo, background=background,
                           offset_sky_background=offset_sky_background)

        if self.config.doWrite:
            dataRef.put(charRes.sourceCat, "icSrc")
            if self.config.doWriteExposure:
                dataRef.put(charRes.exposure, "icExp")
                dataRef.put(charRes.background, "icExpBackground")

        return charRes

    # We need to override the run method in order to return the psfSuccess flag
    @pipeBase.timeMethod
    def run(self, exposure, exposureIdInfo=None, background=None, offset_sky_background=None):
        """
        Huntsman method overrides:
          - Return the psfSuccess flag
          - Implement offset sky background subtraction
        """
        self._frame = self._initialFrame  # reset debug display frame

        if not self.config.doMeasurePsf and not exposure.hasPsf():
            self.log.warn("Source catalog detected and measured with placeholder or default PSF")
            self.installSimplePsf.run(exposure=exposure)

        if exposureIdInfo is None:
            exposureIdInfo = ExposureIdInfo()

        # This is a Huntsman modification
        if offset_sky_background:
            exposure_arr = exposure.getImage().getArray()
            exposure_arr -= offset_sky_background.getImage().getArray()
        else:
            # Measure and subtract an initial estimate of background level
            # Note this implicitly modifies the exposure
            background = self.background.run(exposure).background

        # Detect sources and measure the PSF
        psfIterations = self.config.psfIterations if self.config.doMeasurePsf else 1
        for i in range(psfIterations):

            dmeRes = self.detectMeasureAndEstimatePsf(
                exposure=exposure, exposureIdInfo=exposureIdInfo, background=background)

            # Get summary statistics for this iteration
            psf = dmeRes.exposure.getPsf()
            psfSigma = psf.computeShape().getDeterminantRadius()
            psfDimensions = psf.computeImage().getDimensions()

            if offset_sky_background:
                medBackground = np.median(offset_sky_background.getImage().getArray())
            else:
                medBackground = np.median(dmeRes.background.getImage().getArray())

            self.log.info("iter %s; PSF sigma=%0.2f, dimensions=%s; median background=%0.2f" %
                          (i + 1, psfSigma, psfDimensions, medBackground))

        self.display("psf", exposure=dmeRes.exposure, sourceCat=dmeRes.sourceCat)

        # perform final repair with final PSF
        self.repair.run(exposure=dmeRes.exposure)
        self.display("repair", exposure=dmeRes.exposure, sourceCat=dmeRes.sourceCat)

        # perform final measurement with final PSF
        self.measurement.run(measCat=dmeRes.sourceCat, exposure=dmeRes.exposure,
                             exposureId=exposureIdInfo.expId)
        if self.config.doApCorr:
            apCorrMap = self.measureApCorr.run(exposure=dmeRes.exposure,
                                               catalog=dmeRes.sourceCat).apCorrMap
            dmeRes.exposure.getInfo().setApCorrMap(apCorrMap)
            self.applyApCorr.run(catalog=dmeRes.sourceCat,
                                 apCorrMap=exposure.getInfo().getApCorrMap())
        self.catalogCalculation.run(dmeRes.sourceCat)

        self.display("measure", exposure=dmeRes.exposure, sourceCat=dmeRes.sourceCat)

        return pipeBase.Struct(
            exposure=dmeRes.exposure,
            sourceCat=dmeRes.sourceCat,
            background=dmeRes.background,
            psfCellSet=dmeRes.psfCellSet,
            characterized=dmeRes.exposure,
            backgroundModel=dmeRes.background,
            psfSuccess=dmeRes.psfSuccess
        )

    # Override this method to put the try except around PSF measurement and return success flag
    @pipeBase.timeMethod
    def detectMeasureAndEstimatePsf(self, exposure, exposureIdInfo, background):
        """!Perform one iteration of detect, measure and estimate PSF

        Performs the following operations:
        - if config.doMeasurePsf or not exposure.hasPsf():
            - install a simple PSF model (replacing the existing one, if need be)
        - interpolate over cosmic rays with keepCRs=True
        - estimate background and subtract it from the exposure
        - detect, deblend and measure sources, and subtract a refined background model;
        - if config.doMeasurePsf:
            - measure PSF
        @param[in,out] exposure  exposure to characterize (an lsst.afw.image.ExposureF or similar)
            The following changes are made:
            - update or set psf
            - update detection and cosmic ray mask planes
            - subtract background
        @param[in] exposureIdInfo  ID info for exposure (an lsst.obs_base.ExposureIdInfo)
        @param[in,out] background  initial model of background already subtracted from exposure
            (an lsst.afw.math.BackgroundList).
        @return pipe_base Struct containing these fields, all from the final iteration
        of detect sources, measure sources and estimate PSF:
        - exposure  characterized exposure; image is repaired by interpolating over cosmic rays,
            mask is updated accordingly, and the PSF model is set
        - sourceCat  detected sources (an lsst.afw.table.SourceCatalog)
        - background  model of background subtracted from exposure (an lsst.afw.math.BackgroundList)
        - psfCellSet  spatial cells of PSF candidates (an lsst.afw.math.SpatialCellSet)
        """
        # Install a simple PSF model, if needed or wanted
        if not exposure.hasPsf() or (self.config.doMeasurePsf and self.config.useSimplePsf):
            self.log.warn("Source catalog detected and measured with placeholder or default PSF")
            self.installSimplePsf.run(exposure=exposure)

        # Run repair, but do not interpolate over cosmic rays (do that later)
        if self.config.requireCrForPsf:
            self.repair.run(exposure=exposure, keepCRs=True)
        else:
            try:
                self.repair.run(exposure=exposure, keepCRs=True)
            except LengthError:
                self.log.warn("Skipping cosmic ray detection: Too many CR pixels (max %0.f)" %
                              self.config.repair.cosmicray.nCrPixelMax)

        self.display("repair_iter", exposure=exposure)

        if background is None:
            background = BackgroundList()

        # sourceIdFactory = exposureIdInfo.makeSourceIdFactory()
        sourceIdFactory = IdFactory.makeSource(exposureIdInfo.expId, exposureIdInfo.unusedBits)
        table = SourceTable.make(self.schema, sourceIdFactory)
        table.setMetadata(self.algMetadata)

        # Detect sources
        detRes = self.detection.run(table=table, exposure=exposure, doSmooth=True)
        sourceCat = detRes.sources
        if detRes.fpSets.background:
            for bg in detRes.fpSets.background:
                background.append(bg)

        # Deblend sources
        if self.config.doDeblend:
            self.deblend.run(exposure=exposure, sources=sourceCat)

        # Measure sources
        self.measurement.run(measCat=sourceCat, exposure=exposure, exposureId=exposureIdInfo.expId)

        measPsfRes = pipeBase.Struct(cellSet=None)
        psfSuccess = True

        # Attempt to measure the PSF
        if self.config.doMeasurePsf:
            if self.measurePsf.usesMatches:
                matches = self.ref_match.loadAndMatch(exposure=exposure,
                                                      sourceCat=sourceCat).matches
            else:
                matches = None
            try:
                measPsfRes = self.measurePsf.run(exposure=exposure, sources=sourceCat,
                                                 matches=matches, expId=exposureIdInfo.expId)
            except Exception:
                psfSuccess = False

        self.display("measure_iter", exposure=exposure, sourceCat=sourceCat)

        return pipeBase.Struct(
            exposure=exposure,
            sourceCat=sourceCat,
            background=background,
            psfCellSet=measPsfRes.cellSet,
            psfSuccess=psfSuccess
        )
