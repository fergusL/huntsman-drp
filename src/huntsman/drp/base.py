""" Base class for all huntsman-drp classes which provides the default logger and config."""

from huntsman.drp.core import get_config, get_logger


class HuntsmanBase():
    def __init__(self, config=None, logger=None):
        self.logger = get_logger() if logger is None else logger
        self.config = get_config() if config is None else config
