from tempfile import NamedTemporaryFile
from contextlib import suppress
import numpy as np
import pandas as pd
from astroquery.utils.tap.core import TapPlus
from huntsman.drp.base import HuntsmanBase


class TapReferenceCatalogue(HuntsmanBase):
    """ """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Extract attribute values from config
        self._cone_search_radius = self.config["refcat"]["cone_search_radius"]
        self._ra_key = self.config["refcat"]["ra_key"]
        self._dec_key = self.config["refcat"]["dec_key"]
        self._unique_key = self.config["refcat"]["unique_source_key"]

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
            pandas.DataFrame: The source catalogue.
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
        self._tap.launch_job_async(query, dump_to_file=True, output_format="csv",
                                   output_file=filename)
        return pd.read_csv(filename)

    def create_refcat(self, ra_list, dec_list, filename=None):
        """
        Create the master reference catalogue with no source duplications.

        Args:
            ra_list (iterable): List of RA in J2000 degrees.
            dec_list (iterable): List of Dec in J2000 degrees.
            filename (string, optional): Filename to save output catalogue.

        Returns:
            pandas.DataFrame: The reference catalogue.
        """
        result = None
        with NamedTemporaryFile(delete=True) as tempfile:
            for ra, dec in zip(ra_list, dec_list):
                # Do the cone search and get result
                df = self.cone_search(ra, dec, filename=tempfile.name)
                # First iteration
                if result is None:
                    result = df
                    continue
                # Remove existing sources & concat
                is_new = np.isin(df[self._unique_key].values, result[self._unique_key].values,
                                 invert=True)
                result = pd.concat([result, df[is_new]], ignore_index=False)
        self.logger.debug(f"{result.shape[0]} sources in reference catalogue.")
        if filename is not None:
            result.to_csv(filename)
        return result
