import os
import time
import numpy as np

from huntsman.drp.services.calexp import CalexpQualityMonitor


def test_calexp_quality_monitor(exposure_collection_real_data, master_calib_collection_real_data,
                                config):
    """ Test that the quality monitor is able to calculate and archive calexp metrics. """
    refcat_filename = os.path.join(config["directories"]["testdata"], "refcat.csv")

    n_to_process = exposure_collection_real_data.count_documents({"dataType": "science"})
    m = CalexpQualityMonitor(exposure_collection=exposure_collection_real_data,
                             refcat_filename=refcat_filename, status_interval=5, queue_interval=5,
                             calib_collection=master_calib_collection_real_data)
    m.start()
    i = 0
    timeout = 180
    try:
        while (i < timeout) and (m.status["processed"] != n_to_process) and m.is_running:
            i += 1
            time.sleep(1)
        if i == timeout:
            raise RuntimeError(f"Timeout while waiting for processing of {n_to_process} images.")
        if not m.is_running:
            raise RuntimeError("Calexp monitor has stopped running.")
        for md in exposure_collection_real_data.find({"dataType": "science"}):
            assert "calexp" in md["quality"].keys()
        for metric_value in md["quality"]["calexp"].values():
            assert np.isfinite(metric_value)
        assert m.status["failed"] == 0
    finally:
        m.stop()
        assert not m.is_running
