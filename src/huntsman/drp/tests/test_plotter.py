import os
import time
import pytest

from huntsman.drp.services.plotter import PlotterService


@pytest.fixture(scope="function")
def plotter_service(config, exposure_collection_real_data):

    ps = PlotterService(config=config)
    yield ps

    # Make sure the service is stopped
    ps.stop()

    # Cleanup the image dir ready for other tests
    for p in ps.plotters:
        for fname in os.listdir(p.image_dir):
            if fname.endswith(".png"):
                os.remove(os.path.join(p.image_dir, fname))


def test_plotter_service(plotter_service):
    assert len(plotter_service.plotters) == 2

    # Start the service
    plotter_service.start()

    # Wait for plots to be created
    time.sleep(10)

    # Check that the files exist
    for p in plotter_service.plotters:
        assert os.path.isdir(p.image_dir)
        n_images = sum([len(_) for _ in p._plot_configs.values()])
        n_actual = len([_ for _ in os.listdir(p.image_dir) if _.endswith(".png")])

        # Strictly, this should be an exact equality. However we don't necessarily know how many
        # cameras / filters there are, so this will do for now.
        assert n_actual >= n_images

    # Check that the service is still running
    assert plotter_service.is_running

    # Check we can stop the service
    plotter_service.stop()
    assert not plotter_service.is_running

    # Check that calling stop again does not raise an error
    plotter_service.stop()
