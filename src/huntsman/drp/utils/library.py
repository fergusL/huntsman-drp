from astropy.utils import resolve_name


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
