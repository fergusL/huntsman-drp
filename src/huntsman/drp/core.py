import os
import sys
import logging
from logging import handlers
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


def get_logger(backupCount=5, maxBytes=256000000):
    """ Get the huntsman-drp logger.
    Args:
        backupCount (int, optional): The maximum number of old log files to keep. Default 5.
        maxBytes (int, optional): The maximum size of each log file. Default 256MB.
    Returns:
        logging.logger: The logger object.
    """
    # Get the logs directory
    try:
        logdir = os.environ["HUNTSMAN_DRP_LOGS"]
    except KeyError:
        try:
            logdir = os.path.join(os.environ["HUNTSMAN_DRP"], "logs")
        except KeyError:
            raise KeyError("Neither HUNTSMAN_DRP_LOGS or HUNTSMAN_DRP environment variables set."
                           " Unable to determine log directory.")

    # Make sure log directory exists
    os.makedirs(logdir, exist_ok=True)

    # Create a logger object
    logger = logging.getLogger("huntsman-drp")

    if not logger.handlers:  # If the logger is not initialised already
        logger.setLevel(logging.DEBUG)

        # Create log formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Create an info-level handler that will be printed in the terminal
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Create an info-level handler that will be written to a log file
        filename_info = os.path.join(logdir, "huntsman-drp-info.log")
        handler = handlers.RotatingFileHandler(filename_info, backupCount=backupCount,
                                               maxBytes=maxBytes)
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Create a debug-level handler that will be written to a log file
        filename_debug = os.path.join(logdir, "huntsman-drp-debug.log")
        handler = handlers.RotatingFileHandler(filename_debug, backupCount=backupCount,
                                               maxBytes=maxBytes)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Create a warning-level handler that will be written to a log file
        filename_warning = os.path.join(logdir, "huntsman-drp-warning.log")
        handler = handlers.RotatingFileHandler(filename_warning)
        handler.setLevel(logging.WARNING)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
