"""
File to house core functionality like logging and config.
"""
import os
import sys
import logging
import yaml
from contextlib import suppress
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


def _parse_directories(d):
    """ Recursively parse directories, expanding environment variables. """
    for k, v in d.items():
        if isinstance(v, abc.Mapping):
            _parse_directories(v)
        else:
            d[k] = os.path.expandvars(v)
    return d


def get_config(config_dir=None, ignore_local=False, parse=True, testing=False):
    """

    """
    try:
        rootdir = os.environ["HUNTSMAN_DRP"]
    except KeyError:
        raise KeyError("HUNTSMAN_DRP environment variable not set."
                       " Unable to determine config directory.")
    if config_dir is None:
        config_dir = os.path.join(os.environ["HUNTSMAN_DRP"], "config")
    config = _load_yaml(os.path.join(config_dir, "config.yaml"))

    # Update the config with testing version
    if testing:
        config_test = _load_yaml(os.path.join(config_dir, "testing.yaml"))
        config = _update_config(config, config_test)

    # Update the config with local version
    if not ignore_local:
        with suppress(FileNotFoundError):
            config_local = _load_yaml(os.path.join(config_dir, "config_local.yaml"))
            config = _update_config(config, config_local)

    # Parse the config
    if parse:
        config["directories"] = _parse_directories(config["directories"])
    config["directories"]["root"] = rootdir

    return config


def get_logger():
    """

    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
