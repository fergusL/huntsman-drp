""" Run the data quality monitor. The task of the data quality monitor is to extract information
like zeropoints and PSF FWHM from screened data, storing the information in the data quality
database table. """
import os
from huntsman.drp.services.calexp import CalexpQualityMonitor
from huntsman.drp.utils.pyro import wait_for_nameserver

if __name__ == "__main__":

    wait_for_nameserver()  # Calexp quality monitor needs refcat

    monitor = CalexpQualityMonitor()

    # Set niceness level
    niceness = monitor.config.get("niceness", None)
    if niceness:
        os.nice(niceness - os.nice(0))

    monitor.start()
