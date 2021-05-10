import os
import yaml
from astropy.utils import resolve_name


def load_yaml(filename):
    """ Load yaml from file.
    Args:
        filename (str): The filename to load.
    Returns:
        d (dict): The loaded yaml dictionary.
    """
    with open(filename, 'r') as f:
        d = yaml.safe_load(f)
    return d


def normalise_path(path):
    """ Normalise a path.
    Args:
        path (str): The path to normalise.
    Returns:
        str: The normalised path.
    """
    return os.path.abspath(os.path.realpath(path))


def load_module(module_name):
    """
    Args:
        module_name (str): Name of module to import.
    Returns:
        module: The imported module.
    Raises:
        ImportError: If module cannot be imported.
    """
    return resolve_name(module_name)
