import os
import time

from huntsman.drp.screener import Screener
from huntsman.drp.utils.screening import SCREEN_SUCCESS_FLAG, screen_success


def test_screener_ingest(tempdir_and_exposure_table_with_uningested_files, config):
    """This test runs on a directory where ~70% of the images have already been
    ingested into the datatable. The files already in the datatable should be
    identified as requiring screening. The uningested files should be ingested
    and then should be picked up for screening as well"""

    tempdir, exposure_table = tempdir_and_exposure_table_with_uningested_files

    n_to_ingest = len(os.listdir(tempdir)) - exposure_table.count_documents()
    n_to_screen = len(os.listdir(tempdir))
    assert n_to_ingest > 0

    screener = Screener(exposure_table=exposure_table, sleep_interval=1, status_interval=1,
                        monitored_directory=tempdir)
    # don't want to run astrometry tasks as tests running in drp-lsst container
    # not the drp container
    screener._raw_metrics = [_ for _ in screener._raw_metrics if _ != "get_wcs"]

    screener.start()
    i = 0
    timeout = 10
    try:
        while (i < timeout):
            cond = screener.status["ingested"] == n_to_ingest
            cond &= screener.status["screened"] == n_to_screen
            cond &= screener.is_running
            if cond:
                break
            i += 1
            time.sleep(1)

        if i == timeout:
            raise RuntimeError(f"Timeout while waiting for processing of {n_to_ingest} images.")

        if not screener.is_running:
            raise RuntimeError("Screener has stopped running.")

        for md in exposure_table.find():
            assert SCREEN_SUCCESS_FLAG in md
            assert "quality" in md

        for metric_value in md["quality"].values():
            assert metric_value is not None

    finally:
        screener.stop()
        assert not screener.is_running
        # check that the expected number of files were ingested
        assert screener.status['ingested'] == n_to_ingest
        # check that the expected number of files were screened
        assert screener.status['screened'] == n_to_screen
        # check that exposure_table entries have been screen successfully
        for md in exposure_table.find():
            assert screen_success(md)
        # TODO: implement a check that all the expected metric keys are present in table
