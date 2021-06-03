""" Huntsman overrides for ProcessCcdTask. """
import lsst.pipe.base as pipeBase
from lsst.pipe.tasks.processCcd import ProcessCcdTask


class HuntsmanProcessCcdTask(ProcessCcdTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # Override method to put try except block around each subtask so we can return as much
    # info as possible even if one of the subtasks fails
    @pipeBase.timeMethod
    def runDataRef(self, sensorRef):
        """Process one CCD
        The sequence of operations is:
        - remove instrument signature
        - characterize image to estimate PSF and background
        - calibrate astrometry and photometry
        @param sensorRef: butler data reference for raw data
        @return pipe_base Struct containing these fields:
        - charRes: object returned by image characterization task; an lsst.pipe.base.Struct
            that will include "background" and "sourceCat" fields
        - calibRes: object returned by calibration task: an lsst.pipe.base.Struct
            that will include "background" and "sourceCat" fields
        - exposure: final exposure (an lsst.afw.image.ExposureF)
        - background: final background model (an lsst.afw.math.BackgroundList)
        """
        self.log.info("Processing %s" % (sensorRef.dataId))

        # Default return values
        exposure = None
        charRes = None
        calibRes = None

        # Instrument signature removal
        isrSuccess = True
        try:
            exposure = self.isr.runDataRef(sensorRef).exposure
        except Exception as err:
            self.log.error(f"Error while runing charImage: {err!r}")
            isrSuccess = False

        # Characterise image
        charSuccess = False
        if isrSuccess:
            try:
                charRes = self.charImage.runDataRef(dataRef=sensorRef, exposure=exposure,
                                                    doUnpersist=False)
                exposure = charRes.exposure
                charSuccess = True
            except Exception as err:
                self.log.error(f"Error while runing charImage: {err!r}")

        # The PSF code is wrapped in a try, except block so we can return the other results
        # We need to explicitly indicate that the charImage task failed if the PSF failed
        if isrSuccess and charSuccess:
            if not charRes.psfSuccess:
                self.log.error("Error while runing charImage PSF estimator")
                charSuccess = False

        # Do image calibration (astrometry + photometry)
        calibSuccess = False
        if self.config.doCalibrate and charSuccess:
            try:
                calibRes = self.calibrate.runDataRef(
                    dataRef=sensorRef, exposure=charRes.exposure, background=charRes.background,
                    doUnpersist=False, icSourceCat=charRes.sourceCat)
                calibSuccess = True
            except Exception as err:
                self.log.error(f"Error while runing calibrate: {err!r}")

        return pipeBase.Struct(
            charRes=charRes,
            calibRes=calibRes,
            exposure=exposure,
            calibSuccess=calibSuccess,
            charSuccess=charSuccess,
            isrSuccess=isrSuccess,
        )
