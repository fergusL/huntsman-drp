import os
import pytest
import logging

from huntsman.drp.core import get_logger


@pytest.fixture(scope="module")
def logger():
    return get_logger()


def test_logger_debug(logger):
    """
    """
    message = "hello from debug"
    logger.debug(message)
    count = 0
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            if handler.level <= logging.DEBUG:
                assert os.path.isfile(handler.baseFilename)
                with open(handler.baseFilename, "r") as f:
                    lines = f.readlines()
                    assert message in lines[-1]
                count += 1
    assert count == 1


def test_logger_info(logger):
    """
    """
    message = "hello from info"
    logger.info(message)
    count = 0
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            if handler.level <= logging.INFO:
                assert os.path.isfile(handler.baseFilename)
                with open(handler.baseFilename, "r") as f:
                    lines = f.readlines()
                    assert message in lines[-1]
                count += 1
    assert count == 2
