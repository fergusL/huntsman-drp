import time
from threading import Thread

import Pyro5.errors
from Pyro5.core import locate_ns
from Pyro5.api import Daemon as PyroDaemon
from Pyro5.nameserver import start_ns_loop

from huntsman.drp.base import HuntsmanBase

# Name server


def wait_for_nameserver(timeout=30, **kwargs):
    """ Simple function to wait until nameserver is available.
    Args:
        timeout (float): The timeout in seconds.
        **kwargs: Parsed to NameServer init function.
    Raises:
        RuntimeError: If the timeout is reached before the NS is available.
    """
    ns = NameServer(**kwargs)
    ns.logger.info(f"Waiting for {timeout}s for nameserver.")
    i = 0
    while not ns.connect(suppress_error=True):
        if i > timeout:
            raise RuntimeError("Timeout while waiting for pyro nameserver.")
        i += 1
        time.sleep(1)


class NameServer(HuntsmanBase):

    """ Class to start or connect to the Pyro nameserver given a config file. """

    def __init__(self, host=None, port=None, connect=True, *args, **kwargs):
        super().__init__(*args, **kwargs)

        try:
            ns_config = self.config["pyro"]
        except KeyError:
            ns_config = {}

        self.host = host if host is not None else ns_config["host"]
        self.port = port if port is not None else ns_config["port"]

        self.name_server = None
        if connect:
            self.connect()

    def connect(self, broadcast=True, suppress_error=False):
        """ Connect to the name server.
        See documentation for Pyro5.core.locate_ns. """
        try:
            self.logger.debug(f'Looking for nameserver on {self.host}:{self.port}')
            self.name_server = locate_ns(host=self.host, port=self.port, broadcast=broadcast)
            self.logger.debug(f'Found Pyro name server: {self.name_server}')
            return True
        except Pyro5.errors.NamingError:
            if not suppress_error:
                self.logger.error("Unable to find nameserver.")
            return False

    def serve(self):
        """ Start the nameserver (blocking). """
        if self.connect(suppress_error=True):
            self.logger.warning("Name server is already running.")
            return False
        else:
            self.logger.info("Starting pyro name server.")
            start_ns_loop(host=self.host, port=self.port, enableBroadcast=True)

    def clear(self):
        """ Remove all objects from name server. """
        for name in self.name_server.list():
            self.logger.debug(f"Removing {name} from name server.")
            self.name_server.remove(name)

# Service


class PyroService(HuntsmanBase):
    """ Class used to start and stop pyro services. """

    def __init__(self, server_instance, pyro_name, host="localhost", port=0, *args, **kwargs):
        """
        Args:
            server_instance (object): The server object to expose with Pyro.
            pyro_name (str): The name to register the service with the name server.
            host (optinal): The hostname. default: localhost.
            port (int, optional): The port. Default 0 (auto-assign).
            *args, **kwargs: Parsed to HuntsmanBase init function.
        """

        super().__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.pyro_name = pyro_name

        self._server_instance = server_instance

        # Create the thread which will run the pyro request loop
        self._request_loop_thread = Thread(target=self._start_request_loop)

        # This flag can be used to stop the request loop
        self._continue_loop = True

    def start(self):
        """ Start the pyro request loop. """
        self._continue_loop = True
        self._request_loop_thread.start()

    def stop(self):
        """ Stop the pyro request loop. """
        self._continue_loop = False
        self._request_loop_thread.join()

    def _start_request_loop(self):
        """ Create the pyro daemon, register with nameserver, start request loop. """

        # Connect to the nameserver
        # NOTE: This must be done within the thread
        ns = NameServer(config=self.config, logger=self.logger)

        with PyroDaemon(host=self.host, port=self.port) as daemon:

            # Register the instance
            uri = daemon.register(self._server_instance)

            # Register on the nameserver
            self.logger.info(f"Registering {self._server_instance} with URI {uri} as"
                             f" {self.pyro_name}.")
            ns.name_server.register(self.pyro_name, uri, safe=True)

            # Start the request loop
            # This blocks until loopCondition is False
            self.logger.info(f"Starting request loop for {self._server_instance}.")
            daemon.requestLoop(loopCondition=lambda: self._continue_loop)

            self.logger.info(f"Stopping request loop for {self._server_instance}")
