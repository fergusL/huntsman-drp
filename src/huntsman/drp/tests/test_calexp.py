import time

from huntsman.drp.services.calexp import CalexpQualityMonitor


def test_calexp_quality_monitor(exposure_collection_real_data, master_calib_collection_real_data,
                                testing_refcat_server, config):
    """ Test that the quality monitor is able to calculate and archive calexp metrics. """

    # Make sure the service uses the correct collections
    raw_name = exposure_collection_real_data.collection_name
    calib_name = master_calib_collection_real_data.collection_name
    config["mongodb"]["collections"]["RawExposureCollection"]["name"] = raw_name
    config["mongodb"]["collections"]["MasterCalibCollection"]["name"] = calib_name

    n_to_process = exposure_collection_real_data.count_documents({"dataType": "science"})

    m = CalexpQualityMonitor(status_interval=5, queue_interval=5, config=config)
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
            assert "calexp" in md["metrics"].keys()

            for metric_value in md["metrics"]["calexp"].values():
                assert metric_value is not None

        assert m.status["failed"] == 0
    finally:
        m.stop()
        assert not m.is_running

    # Test delete calexp metrics
    exposure_collection_real_data.clear_calexp_metrics()
    for md in exposure_collection_real_data.find({"dataType": "science"}):
        exposure_collection_real_data.logger.info(f"{md._document}")
        assert "calexp" not in md["metrics"].keys()
