import os
from loguru import logger as LOGGER
import yaml
from contextlib import suppress
from collections import abc

FILE_LOG_LEVELS = ("DEBUG", "INFO", "WARNING")


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


def get_logdir():
    """ Get the huntsman-drp log directory.
    """
    # Get the logs directory
    try:
        logdir = os.environ["HUNTSMAN_LOG_DIR"]
    except KeyError:
        try:
            logdir = os.path.join(os.environ["HUNTSMAN_DRP"], "logs")
        except KeyError:
            raise KeyError("Neither HUNTSMAN_LOG_DIR or HUNTSMAN_DRP environment variables set."
                           " Unable to determine log directory.")
    return logdir


def get_logger(rotation="500 MB", retention=5):
    """ Get the huntsman-drp logger.
    See documentation for loguru.Logger.add.
    """
    # Get the logs directory
    logdir = get_logdir()

    # Make sure log directory exists
    os.makedirs(logdir, exist_ok=True)

    # Add files to log
    # Note: Use enqueue=True to make it work with multiprocessing
    for level in FILE_LOG_LEVELS:
        filename = os.path.join(logdir, f"hunts-drp-{level.lower()}.log")

        # Make sure the file has not already been added
        # TODO: Check if there is a cleaner way of doing this!
        duplicate = False
        for handler in LOGGER._core.handlers.values():
            with suppress(AttributeError):
                if filename == handler._sink._file_path:
                    duplicate = True
                    break
        if not duplicate:
            LOGGER.add(filename, level=level, rotation=rotation, retention=retention, enqueue=True)

    return LOGGER
