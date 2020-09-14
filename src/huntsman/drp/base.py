import os
import sys
import logging
import yaml
from collections import abc


def _load_yaml(filename):
    with open(filename, 'r') as f:
        config = yaml.safe_load(f)
    return config


def _update_config(d, u):
    """Recursively update nested dictionary d with u."""
    for k, v in u.items():
        if isinstance(v, abc.Mapping):
            d[k] = _update_config(d.get(k, {}), v)
        else:
            d[k] = v
    return d


class HuntsmanBase():
    """ Base class to setup config and logger."""

    def __init__(self, config=None, logger=None):

        # Load the logger
        if logger is None:
            logger = self._get_logger()
        self.logger = logger

        # Load the config
        if config is None:
            config = self._load_config()
        self.config = config

    def _load_config(self, config_dir=None):
        """Load the config file"""
        dir = os.path.join(os.environ["HUNTSMAN_DRP"], "conf_files")
        config = _load_yaml(os.path.join(dir, "config.yaml"))
        # Update the config with local version
        try:
            config_local = _load_yaml(os.path.join(dir, "config_local.yaml"))
            config = _update_config(config, config_local)
        except FileNotFoundError:
            pass
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
