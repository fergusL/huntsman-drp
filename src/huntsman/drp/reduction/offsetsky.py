"""
https://github.com/lsst/meas_algorithms/blob/master/python/lsst/meas/algorithms/subtractBackground.py
https://github.com/lsst/afw/blob/master/python/lsst/afw/math/_backgroundList.py
https://github.com/lsst/meas_algorithms/blob/master/python/lsst/meas/algorithms/detection.py

NOTES:
  - RMS level used in source detection is measured from the image *not* the sky background
"""
from copy import deepcopy
from datetime import timedelta

import numpy as np

import lsst.afw.image

from huntsman.drp.reduction.base import ReductionBase
from huntsman.drp.reduction.lsst import LsstReduction

EXTRA_CALEXP_CONFIG = {"charImage.useOffsetSky": True,
                       "charImage.detection.reEstimateBackground": False,
                       "calibrate.detection.reEstimateBackground": False}

EXTRA_CALEXP_CONFIG_SKY = {"calibrate.doPhotoCal": False}


class OffsetSkyReduction(LsstReduction):
    """ Data reduction using offset sky frames to estimate background for science images. """

    def __init__(self, sky_query, timedelta_minutes, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._sky_query = sky_query
        self._timedelta_minutes = timedelta_minutes

        self.sky_docs = {}

        # Make sure required reduction kwargs are set for sky calexps
        self._calexp_kwargs_sky = deepcopy(self._calexp_kwargs)
        extra_config_sky = deepcopy(self._calexp_kwargs_sky.get("extra_config", {}))
        extra_config_sky.update(EXTRA_CALEXP_CONFIG_SKY)
        self._calexp_kwargs_sky["extra_config"] = extra_config_sky

        # Make sure required reduction kwargs are set for science calexps
        extra_config = self._calexp_kwargs.get("extra_config", {})
        extra_config.update(EXTRA_CALEXP_CONFIG)
        self._calexp_kwargs["extra_config"] = extra_config

        # We need to allow LSST to overwrite the existing config file
        self._calexp_kwargs["clobber_config"] = True

    def prepare(self):
        """ Override method to get matching sky offset docs and their associated calibs.
        """
        # Use base prepare method to set science docs, calibs and make reference catalogue
        ReductionBase.prepare(self)

        for doc in self.science_docs:
            # Get background docs
            self.sky_docs[doc] = self._get_matching_sky_docs(doc)

            all_sky_docs = self._get_all_sky_docs()  # List rather than dict

            # Update set of calibs so we can reduce the background docs
            calib_docs = self._get_calibs(all_sky_docs)
            for datasetType, docs in calib_docs.items():
                self.calib_docs[datasetType].update(docs)

        # Ingest raw data, calibs and refcat
        # Note: Only the science docs need a refcat because we don't need to calibrate the sky ones
        super().prepare(call_super=False)

        # Ingest extra sky docs
        self._butler_repo.ingest_raw_data([d["filename"] for d in all_sky_docs])

    def reduce(self):
        """ Override method to measure the offset sky backgrounds before processing. """

        self.logger.info(f"Measuring sky backgrounds for {len(self.sky_docs)} sky offset images.")
        self.measure_backgrounds()

        self.logger.info(f"Making master sky images for {len(self.science_docs)} science images.")
        for doc in self.science_docs:
            self.make_master_background(doc, self.sky_docs[doc])

        super().reduce()

    def measure_backgrounds(self):
        """ Measure background for each sky image. """

        # Get dataIds to reduce
        dataIds = [self._butler_repo.document_to_dataId(doc) for doc in self._get_all_sky_docs()]

        # Process the dataIds
        self._butler_repo.make_calexps(dataIds=dataIds, **self._calexp_kwargs_sky)

    def make_master_background(self, document, matching_sky_docs, rerun="default"):
        """ Get a master background image for the specific document and persist using butler. """

        # Get background images from LSST
        bg_list = []
        for doc in matching_sky_docs:
            dataId = self._butler_repo.document_to_dataId(doc)
            bg = self._butler_repo.get("calexpBackground", dataId=dataId, rerun=rerun)

            # Get the full-sized BG image as a np.array
            bg_list.append(bg.getImage().getArray())

        # Combine the sky images
        bg_master = np.median(bg_list, axis=1)

        # Package into an LSST-friendly object
        image = lsst.afw.image.ImageF(bg_master)
        exposure = lsst.afw.image.ExposureF(image.getBBox())
        exposure.setImage(image)

        # Use butler to persist the image using a custom datasetType (specified in policy)
        dataId = self._butler_repo.document_to_dataId(document)
        butler = self._butler_repo.get_butler(rerun=rerun)
        dataRef = butler.dataRef(datasetType="raw", dataId=dataId)
        dataRef.put(exposure, "offsetBackground")

    def _get_matching_sky_docs(self, document):
        """ Get list of documents to measure the offset sky background with.
        Args:
            document (RawExposureDocument): The raw exposure document to match with.
        Returns:
            list of RawExposureDocument: The matching documents.
        """
        td = timedelta(minutes=self._timedelta_minutes)
        date_min = document["date"] - td
        date_max = document["date"] + td

        matches = self._exposure_collection.find(date_min=date_min, date_max=date_max,
                                                 **self._sky_query)

        if not matches:
            raise RuntimeError(f"No matching offset sky images for {document}.")

        self.logger.debug(f"Found {len(matches)} matching offset sky images for {document}.")

        return matches

    def _get_all_sky_docs(self):
        """ Get all sky documents in a set rather than a nested dictionary. """
        all_sky_docs = set()
        for sky_docs in self.sky_docs.values():
            all_sky_docs.update(sky_docs)
        return all_sky_docs
