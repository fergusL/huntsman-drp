import os
import time
import yaml
import pytest

from huntsman.drp.core import get_config, get_logger, get_logdir, FILE_LOG_LEVELS


@pytest.fixture(scope="module")
def logger():
    return get_logger()


@pytest.fixture(scope="module")
def config_dict():
    return {"a": 1, "b": 2, "c": 3, "directories": {"home": "${HOME}"}}


@pytest.fixture(scope="module")
def config_dict_local():
    return {"c": 4, "d": 5}


def test_logger(logger):
    """
    """
    time.sleep(3)  # There may be some async processes writing to the logs...

    for level in FILE_LOG_LEVELS:

        # Log a message at this level
        message = f"hello from {level}"
        getattr(logger, level.lower())(message)

        filename = os.path.join(get_logdir(), f"hunts-drp-{level.lower()}.log")
        assert os.path.isfile(filename)

        with open(filename, "r") as f:
            lines = f.readlines()
            assert message in lines[-1]


@pytest.fixture(scope="module")
def config_dir(config_dict, config_dict_local, tmpdir_factory):
    """Write configs to file in a tempdir"""
    dir = str(tmpdir_factory.mktemp("test_config_dir"))
    with open(os.path.join(dir, "config.yaml"), 'w') as f:
        yaml.dump(config_dict, f)
    with open(os.path.join(dir, "config_local.yaml"), 'w') as f:
        yaml.dump(config_dict_local, f)
    return dir


def test_get_config(config_dict, config_dir):
    """Test we can load the config file."""
    config = get_config(config_dir=config_dir, ignore_local=True)
    assert len(config) == len(config_dict)
    for key, value in config_dict.items():
        if key != "directories":
            assert config[key] == value
    assert config["directories"]["home"] == os.environ["HOME"]


def test_load_local_config(config_dict, config_dict_local, config_dir):
    """Test we can load the config file and the local config file."""
    config = get_config(config_dir=config_dir, ignore_local=False)
    for key, value in config_dict_local.items():
        if key != "directories":
            assert config[key] == value
    for key, value in config_dict.items():
        if key not in config_dict_local.keys():
            if key != "directories":
                assert config[key] == value
    assert config["directories"]["home"] == os.environ["HOME"]
