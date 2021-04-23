import os
import pytest

from huntsman.drp.core import get_logger, get_logdir, FILE_LOG_LEVELS


@pytest.fixture(scope="module")
def logger():
    return get_logger()


def test_logger(logger):
    """
    """
    for level in FILE_LOG_LEVELS:

        # Log a message at this level
        message = f"hello from {level}"
        getattr(logger, level.lower())(message)

        filename = os.path.join(get_logdir(), f"hunts-drp-{level.lower()}.log")
        assert os.path.isfile(filename)

        with open(filename, "r") as f:
            lines = f.readlines()
            assert message in lines[-1]
