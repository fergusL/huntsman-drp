import os
import time

from huntsman.drp.quality.screening import Screener


def test_screener_ingest(tempdir_and_exposure_table_with_uningested_files, config):
    """This test runs on a directory where ~70% of the images have already been
    ingested into the datatable. The files already in the datatable should be
    identified as requiring screening. The uningested files should be ingested
    and then should be picked up for screening as well"""

    tempdir, exposure_table = tempdir_and_exposure_table_with_uningested_files

    n_to_ingest = len(os.listdir(tempdir)) - exposure_table.count_documents()
    n_to_screen = len(os.listdir(tempdir))
    assert n_to_ingest > 0

    screener = Screener(exposure_table=exposure_table, sleep_interval=0.1, status_interval=0.1,
                        monitored_directory=tempdir)
    # don't want to run astrometry tasks as tests running in drp-lsst container
    # not the drp container
    screener._raw_metrics = [_ for _ in screener._raw_metrics if _ != "has_wcs"]

    screener.start()
    i = 0
    timeout = 5
    try:
        while (i < timeout) and (screener.status["ingested"] != n_to_ingest) and \
                (screener.status["screened"] != n_to_screen) and screener.is_running:
            i += 1
            time.sleep(1)
        if i == timeout:
            raise RuntimeError(
                f"Timeout while waiting for processing of {n_to_ingest} images.")
        if not screener.is_running:
            raise RuntimeError("Screener has stopped running.")
        for md in exposure_table.find():
            assert "rawexp" in md["quality"].keys()
            for key in md["quality"]["rawexp"].keys():
                if key != "get_wcs":
                    assert md["quality"]["rawexp"][key] is not None
    finally:
        screener.stop()
        assert not screener.is_running
        # check that the expected number of files were ingested
        assert screener.status['ingested'] == n_to_ingest
        # check that the expected number of files were screened
        assert screener.status['screened'] == n_to_screen
        # check that exposure_table entries have been screen successfully
        for md in exposure_table.find():
            assert md['quality']['screen_success']
        # TODO: implement a check that all the expected metric keys are present in table
