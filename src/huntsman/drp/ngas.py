import requests
from huntsman.drp.base import HuntsmanBase


class NgasClient(HuntsmanBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._server = self.config["ngas"].get("server", "localhost")
        self._port = self.config["ngas"].get("port", 7778)

    def push(self, filename_local, filename_ngas=None):
        """
        Push file using QARCHIVE command.

        Args:
            filename_local (str): Local name of the file to push.
            filename_ngas (str): The name of the file stored by NGAS.
        """
        if filename_ngas is None:
            filename_ngas = filename_local
        params = dict(filename=filename_ngas, ignore_arcfile=1, format="json")
        with open(filename_local, 'rb') as data:
            response = self._send_command("QARCHIVE", parameters=params, data=data)
        return response

    def query_files(self):
        """
        Query available files.

        Returns:
            request.Response
        """
        params = dict(format="json", query="files_list")
        return self._send_command("QUERY", parameters=params)

    def _send_command(self, command, parameters=None, **kwargs):
        """
        Send command to NGAS and return response.

        Args:
            command (str): A valid NGAS command. See
                https://ngas.readthedocs.io/en/master/commands-index.html.
            parameters (str): Parameters of the NGAS command.

        Returns:
            request.Response
        """
        command = f"http://{self._server}:{self._port}/{command}"
        if parameters is not None:
            for key, value in parameters.items():
                command += f"&{key}={value}"
        self.logger.debug(f"Posting NGAS command: {command}")
        response = requests.post(command, **kwargs)
        return response
