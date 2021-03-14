""" Run the data quality monitor. The task of the data quality monitor is to extract information
like zeropoints and PSF FWHM from screened data, storing the information in the data quality
database table. """
from huntsman.drp.quality.monitor import CalexpQualityMonitor

if __name__ == "__main__":

    monitor = CalexpQualityMonitor()
    monitor.start()
