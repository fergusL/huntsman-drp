from contextlib import suppress
from astroquery.utils.tap.core import TapPlus
from huntsman.utils.base import HuntsmanBase


class TapReferenceCatalogue(HuntsmanBase):
    """ """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Extract attribute values from config
        self._cone_search_radius = self.config["refcat"]["cone_search_radius"]
        self._ra_key = self.config["refcat"].get("ra_key", "raj2000")
        self._dec_key = self.config["refcat"].get("ra_key", "dej2000")
        self._tap_url = self.config["refcat"]["tap_url"]
        self._tap_table = self.config["refcat"]["tap_table"]
        self._tap_limit = self.config["refcat"].get("tap_limit", None)
        self._parameter_ranges = self.config["refcat"]["parameter_ranges"]

        # Create the tap object
        self._tap = TapPlus(url=self._tap_url)

    def cone_search(self, ra, dec, filename):
        """
        Query the reference catalogue, saving output to a .csv file.

        Args:
            ra: RA of the centre of the cone search in J2000 degrees.
            dec: Dec of the centre of the cone search in J2000 degrees.
            filename: Filename of the returned .csv file.

        Returns:
            An astroquery.Job object.
        """
        query = f"SELECT * FROM {self._tap_table}"

        # Apply cone search
        query += (f" WHERE 1=CONTAINS(POINT('ICRS', {self._ra_key}, {self._dec_key}),"
                  f" CIRCLE('ICRS', {ra}, {dec}, {self._cone_search_radius}))")

        # Apply parameter ranges
        for param, prange in self._parameter_ranges.items():
            with suppress(KeyError):
                query += f" AND {param} >= {prange['lower']}"
            with suppress(KeyError):
                query += f" AND {param} < {prange['upper']}"

        # Apply limit on number of returned rows
        if self._tap_limit is not None:
            query += f" LIMIT {int(self._tap_limit)}"

        # Start the query
        self.logger.debug(f"Cone search command: {query}.")
        job = self._tap.launch_job_async(query, dump_to_file=True, output_format="csv",
                                         output_file=filename)
        return job
