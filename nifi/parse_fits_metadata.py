"""Python script to output metadata as json given an input filename.
"""
import json
import argparse
from bson import json_util

from huntsman.drp.fitsutil import read_fits_header, FitsHeaderTranslator


def parse_file(filename, print_stdout=True):
    """ Print to stdout FitsHeaderTranslator-parsed fits header for use in nifi data archive
    system.
    Args:
        filename (str): Filename of fits file. Can be compressed.
        print_stdout (bool, optional): Print to stdout or not.
    """
    # Read the header
    header = read_fits_header(filename)

    # Parse the header
    meta = FitsHeaderTranslator().parse_header(header)

    # Add the filename to the metadata
    meta["filename"] = filename

    # Print as json, encoding dates properly
    meta_json = json.dumps(meta, default=json_util.default)
    if print_stdout:
        print(meta_json)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('filename', type=str)

    parse_file(parser.parse_args().filename)
