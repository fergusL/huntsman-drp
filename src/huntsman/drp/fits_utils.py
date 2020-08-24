"""

"""
from functools import partial
from huntsman.drp.base import HuntsmanBase


class FitsHeaderTranslator(HuntsmanBase):
    """
    Class used to map information in FITS headers to variables required by the DRP.
    Is used as a base class for `obs_huntsman.HuntsmanParseTask`.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Define direct mappings between fits headers and variable names
        keyword_mapping = self.config["fits_header_mappings"]
        for varname, header_key in keyword_mapping.items():
            funcname = f"translate_{varname}"
            if hasattr(self, funcname):
                raise AttributeError(f"Attribute {funcname} already set in {self}.")
            setattr(self, funcname, partial(self._map_header_key(header_key=header_key)))

    def translate_dataType(self, md):
        """Translate FITS header into dataType: bias, flat or science."""
        if md['IMAGETYP'] == 'Light Frame':
            # The FIELD keyword is set by pocs.observation.field.field_name.
            # For flat fields, this is "Flat Field"
            if md["FIELD"].startswith("Flat Field"):
                dataType = 'flat'
            else:
                dataType = 'science'
        # For Huntsman, we treat all dark frames as biases.
        # The exposure times are used to match biases with science images.
        elif md['IMAGETYP'] == 'Dark Frame':
            dataType = 'bias'
        else:
            raise NotImplementedError(f'IMAGETYP value not recongnised: '
                                      f"{md['IMAGETYP']}")
        return dataType

    def translate_filter(self, md):
        """
        Translate the given filter name to the abstract filter name.
        For Huntsman, we strip of the serial number.
        """
        return "_".join(md["FILTER"].split("_")[:-1])

    def translate_dateObs(self, md):
        """Return the date of observation as a string."""
        return md['DATE-OBS'][:10]

    def translate_visit(self, md):
        """
        Visit should be an integer value to avoid complications.

        For Huntsman purposes, visit should be common to all exposures
        taken simultaneously by the different cameras. This is encoded by the
        time they were observed, provided there is sufficient temporal
        resolution.

        Unique exposures can therefore be identified by visit/ccd pairs.

        Note: There needs to be space in memory for padding of the ccd number
        used in computeExpId.
        """
        date_obs = md['DATE-OBS']  # This is a string
        datestr = ''.join([s for s in date_obs if s.isdigit()])
        assert len(datestr) == 17, "Date string expected to contain 17 numeric characters."
        return int(datestr)

    def translate_ccd(self, md):
        """Get a unique integer corresponding to the CCD."""
        ccd_name = md["INSTRUME"]
        return self.config["camera_mappings"][ccd_name]

    def _map_header_key(self, md, header_key):
        """Generic function to translate header_key to variable."""
        return md[header_key]
