#!/usr/bin/env python
"""
Trigger calexp metrics to be reprocessed.
"""
from contextlib import suppress

from huntsman.drp.collection import RawExposureCollection
from huntsman.drp.services.calexp import CALEXP_METRIC_TRIGGER

UPDATE = {f"metrics.calexp.{CALEXP_METRIC_TRIGGER}": True}

if __name__ == "__main__":

    raw = RawExposureCollection()

    for doc in raw.find({"dataType": "science"}, screen=False, quality_filter=False):
        with suppress(KeyError):
            raw.update_one(doc, UPDATE)
