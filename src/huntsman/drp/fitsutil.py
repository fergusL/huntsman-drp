from copy import copy
from functools import partial
from astropy.io import fits

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import parse_date


def read_fits_data(filename, dtype="float32", **kwargs):
    """ Read fits image into numpy array.
    """
    return fits.getdata(filename, **kwargs).astype(dtype)


def read_fits_header(filename, ext="auto"):
    """ Read the FITS header for a given filename.
    Args:
        filename (str): The filename.
        ext (str or int): Which FITS extension to use. If 'auto' (default), will choose based on
            file extension. If 'all', will recursively extend the header with all extensions.
            Else, will use int(ext) as the ext number.
    Returns:
        dict: The header dictionary.
    """
    if ext == "all":
        header = fits.Header()
        i = 0
        while True:
            try:
                header.extend(fits.getheader(filename, ext=i))
            except IndexError:
                if i > 1:
                    return header
            i += 1
    elif ext == "auto":
        if filename.endswith(".fits"):
            ext = 0
        elif filename.endswith(".fits.fz"):  # <----- CHECK THIS
            ext = 1
        else:
            raise ValueError(f"Unrecognised FITS extension for {filename}.")
    else:
        ext = int(ext)
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
            setattr(self, funcname, partial(self._map_header_key, header_key=header_key))

    def translate_dataType(self, md):
        """ Translate raw FITS header into dataType: bias, dark, flat or science. """

        image_type = md['IMAGETYP']
        field_name = md["FIELD"]

        if image_type == 'Light Frame':
            if field_name.startswith("Flat"):
                dataType = 'flat'
            else:
                dataType = 'science'

        elif image_type == 'Dark Frame':
            if field_name == "Bias":
                dataType = "bias"
            else:
                dataType = "dark"
        else:
            raise NotImplementedError(f"IMAGETYP value not recongnised: {md['IMAGETYP']}")

        return dataType

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
        camera_name = md["CAM-ID"]
        for i, camera_config in enumerate(self.huntsman_config["cameras"]["devices"]):
            if camera_config["camera_name"] == camera_name:
                return i + 1  # i+1 to match camera config in obs_huntsman
        raise RuntimeError(f"No config entry found for camera {camera_name}.")

    def _map_header_key(self, md, header_key):
        """Generic function to translate header_key to variable."""
        return md[header_key]


class FitsHeaderTranslator(FitsHeaderTranslatorBase):
    """Add additional methods here to avoid conflicts with LSST stack."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = self.huntsman_config

    def parse_header(self, header):
        """ Parse header key/values into standardised python objects.
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

        # Explicitly parse the date in specialised format with different key
        result[self._date_key] = self._translate_date(header)

        return result

    def _translate_date(self, header):
        """ Translate the date from the FITS header to a format recognised by pymongo. """
        date_key = self.config["fits_header"]["date_key"]
        date_str = parse_date(header[date_key])
        return date_str
