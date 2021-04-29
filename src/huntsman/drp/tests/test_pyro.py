import pytest
from huntsman.drp.utils.pyro import NameServer, wait_for_nameserver

# NOTE: The nameserver tests rely on a nameserver already running for the time being


def test_wait_for_nameserver(config):

    wait_for_nameserver(timeout=1, config=config)

    with pytest.raises(RuntimeError):
        wait_for_nameserver(timeout=1, host="NotARealHost")


def test_nameserver(config):

    ns = NameServer(config=config)
    assert ns.connect()
    assert not ns.serve()  # Already running

    ns = NameServer(config=config, host="NotARealHost")
    assert not ns.connect()
