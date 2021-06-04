#!/usr/bin/env python
# Based on https://github.com/lsst/pipe_drivers/blob/master/bin.src/constructFlat.py
from huntsman.drp.lsst.tasks.flat import HuntsmanFlatTask
HuntsmanFlatTask.parseAndSubmit()
