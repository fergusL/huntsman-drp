"""
Script to create master calibs in regular intervals. In each time interval, produce master calibs
for today's date and send them to the archive. Only the most recent raw calib data will be used.
Existing calibs for today's date will be overwritten.
"""
import os
import time
import argparse
from datetime import timedelta

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import current_date
from huntsman.drp.datatable import ExposureTable, RawQualityTable
from huntsman.drp.butler import TemporaryButlerRepository


# TODO: Move this class
class RegularCalibMaker(HuntsmanBase):

    _data_type_key = "dataType"
    _filename_key = "filename"

    def __init__(self, sleep_interval=86400, day_range=1000, config=None, logger=None,
                 butler_repository=None, nproc=1, niceness=0, **kwargs):
        super().__init__(config=config, logger=logger, **kwargs)
        self.sleep_interval = sleep_interval
        self.day_range = day_range
        self.rawtable = ExposureTable(config=self.config, logger=self.logger)
        self.dqtable = RawQualityTable(config=self.config, logger=self.logger)
        self._nproc = nproc
        self._calib_types = self.config["calibs"]["types"]
        self._nproc = nproc
        self._niceness = niceness
        if butler_repository is None:
            butler_repository = TemporaryButlerRepository(config=self.config, logger=self.logger)
        self._butler_repository = butler_repository

    def run(self):
        """ Periodically create a new set of master calibs. """
        # TODO: Implement actual queue
        while True:
            date = current_date()
            self.logger.info(f"Queuing new calibs for calibDate: {date}")
            self._run_next(date)
            self.logger.info(f"Sleeping for {self.sleep_interval}s.")
            time.sleep(self.sleep_interval)

    def _run_next(self, date_end):
        """ Run the next set of calibs. """
        # Set niceness level
        n = os.nice(0)
        os.nice(self._niceness - n)

        date_start = date_end - timedelta(days=self.day_range)

        # Get latest files that satisfy screening criteria
        with self._butler_repository as br:
            for calib_type in self._calib_types:
                self.logger.info(f"Retrieving raw files for {self._data_type_key}: {calib_type}.")

                # Get all filenames for this data type
                criteria_raw = {self._data_type_key: calib_type}
                filenames_raw = self.rawtable.find(
                                    date_start=date_start,
                                    date_end=date_end,
                                    criteria=criteria_raw)[self._filename_key].values

                # Specify quality screening cuts
                criteria_qual = {self._filename_key: filenames_raw}
                screenconf = self.config["screening"].get(calib_type, {})
                criteria_qual.update(screenconf)
                self.logger.debug(f"Screening data type={calib_type} with criteria: {screenconf}.")

                # Get filenames that satisfy screening criteria
                query_result = self.dqtable.find(criteria=criteria_qual)
                self.logger.info(f"{query_result.shape[0]} raw files passed screening for"
                                 f" {self._data_type_key}: {calib_type}.")
                filenames = query_result[self._filename_key].values

                # Ingest the files
                br.ingest_raw_data(filenames, ignore_ingested=True)

            # Make master calibs and archive them
            br.make_master_calibs(procs=self._nproc)
            br.archive_master_calibs()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep_interval", type=int, default=86400)
    parser.add_argument("--day_range", type=int, default=30)
    parser.add_argument("--nproc", type=int, default=1)
    parser.add_argument("--niceness", type=int, default=0)
    args = parser.parse_args()

    RegularCalibMaker(sleep_interval=args.sleep_interval, day_range=args.day_range,
                      nproc=args.nproc, niceness=args.niceness).run()
