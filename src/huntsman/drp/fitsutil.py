from copy import copy
from functools import partial
from astropy.io import fits
from huntsman.drp.base import HuntsmanBase


def read_fits_header(filename):
    """ Read the FITS header for a given filename.
    Args:
        filename (str): The filename.
    Returns:
        dict: The header dictionary.
    """
    if filename.endswith(".fits"):
        ext = 0
    elif filename.endswith(".fits.fz"):  # <----- CHECK THIS
        ext = 1
    else:
        raise ValueError(f"Unrecognised FITS extension for {filename}.")
    return fits.getheader(filename, ext=ext)


class FitsHeaderTranslatorBase(HuntsmanBase):
    """
    Class used to map information in FITS headers to variables required by the DRP.
    Is used as a base class for `obs_huntsman.HuntsmanParseTask` and `FitsHeaderTranslator`.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # LSST also uses config, so rename
        self.huntsman_config = copy(self.config)
        self.config = None
        # Define direct mappings between fits headers and variable names
        keyword_mapping = self.huntsman_config["fits_header"]["mappings"]
        for varname, header_key in keyword_mapping.items():
            funcname = f"translate_{varname}"
            if hasattr(self, funcname):
                raise AttributeError(f"Attribute {funcname} already set in {self}.")
            setattr(self, funcname, partial(self._map_header_key, header_key=header_key))

    def translate_dataType(self, md):
        """Translate FITS header into dataType: bias, flat or science."""
        if md['IMAGETYP'] == 'Light Frame':
            # The FIELD keyword is set by pocs.observation.field.field_name.
            # For flat fields, this is "Flat Field"
            if md["FIELD"].startswith("Flat"):
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
        return int(self.huntsman_config["camera_mappings"][ccd_name])

    def _map_header_key(self, md, header_key):
        """Generic function to translate header_key to variable."""
        return md[header_key]


class FitsHeaderTranslator(FitsHeaderTranslatorBase):
    """Add additional methods here to avoid conflicts with LSST stack."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = self.huntsman_config

    def parse_header(self, header):
        """
        Parse header key/values into standardised python variables.

        Args:
            header (dict): Raw FITS header.
        """
        # Copy the whole header
        result = dict()
        for key, value in header.items():
            result[key] = value
        # Also store mappings, overwriting if necessary
        for column in self.config["fits_header"]["required_columns"]:
            result[column] = getattr(self, f"translate_{column}")(header)
        return result
