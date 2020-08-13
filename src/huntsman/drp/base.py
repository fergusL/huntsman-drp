import os
import sys
import logging
import yaml


class HuntsmanBase():
    """ Setup config and logger. Replace with PanBase in future? """

    def __init__(self, config=None, logger=None, config_filename=None):

        # Load the config
        if config is None:
            config = self._load_config(config_filename)
        self.config = config

        # Load the logger
        if logger is None:
            logger = self._get_logger()
        self.logger = logger

    def _load_config(self, filename):
        if filename is None:
            filename = os.path.join(os.environ["HUNTSMAN_DRP"], "conf_files", "config.yaml")
        with open(filename, 'r') as f:
            config = yaml.safe_load(f)
        return config

    def _get_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
