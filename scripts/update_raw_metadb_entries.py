"""Python script to update Huntsman raw metadata database table.
NB script will only refresh/update existing entries.
"""
import os
import json
import glob
import argparse
import logging
import warnings
from multiprocessing import Pool
from functools import partial
import tqdm

from huntsman.drp.fitsutil import read_fits_header, FitsHeaderTranslator
from huntsman.drp.datatable import RawDataTable


def extract_metadata_from_header(fname):
    """Return FitsHeaderTranslator-parsed fits header.

    Args:
        filename (str): Filename of fits file.
    """
    logger = logging.getLogger('raw_md_refresh')
    # Read the header
    try:
        # Some fits files are truncated which triggers an astropy warning
        with warnings.catch_warnings(record=True) as w:
            header = read_fits_header(fname)
            # if any warnings were raised, record in the log
            if len(w) > 0:
                logger.warning(f"Warnings raised in accessing header of {fname}, warning(s): {w}")
    except Exception as e:
        logger.warning(f"Failed to read fits header of {fname}, error: {e!r}")
        return

    # Parse the header
    try:
        meta = FitsHeaderTranslator().parse_header(header)
    except Exception as e:
        logger.warning(f"Failed to parse header of {fname}, error: {e!r}")
        return

    # Add the filename to the metadata
    meta["filename"] = fname

    return meta


def update_raw_datatable(fname):
    """Takes a file and updates its' entry in the raw datatable.

    Args:
        raw_data_table(obj): Huntsman RawDataTable object.
        fname(str): Filename for entry to update.
    """
    logger = logging.getLogger('raw_md_refresh')

    md = extract_metadata_from_header(fname)
    if md is None:
        return

    try:
        rdt = RawDataTable(logger=logger)
        rdt._allow_edits = True
    except Exception as e:
        logger.warning(f"Error accessing raw datatable, error: {e!r}")
        return

    # update the entry for fname, if no entry found check for alternate file extensions
    result = None
    try:
        result = rdt.update_file_data(fname, md)
    except RuntimeError as e:
        logger.info(f"Could not find {fname} in raw datatable,"
                    "checking for alternate file extensions.")
        if fname.endswith('fits'):
            alt_fname = fname.replace('.fits', '.fits.fz')
        if fname.endswith('.fits.fz'):
            alt_fname = fname.replace('.fits.fz', '.fits')
        # try again with alternate fname
        try:
            result = rdt.update_file_data(alt_fname, md)
        except RuntimeError as e:
            logger.warning(f"No entry found in datatable for {fname} or {alt_fname}, error: {e!r}")
        except Exception as e:
            logger.warning(
                f"Failed to update datatable entry for {fname}, error: {e!r}")
    except Exception as e:
        logger.warning(f"Failed to update datatable entry for {fname}, error: {e!r}")

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, default="./",
                        help="Top level `images/` directory of huntsman data repository.")
    parser.add_argument('--nproc', type=int, default=1,
                        help="Number of processors to use in multiprocessing.")
    args = parser.parse_args()

    logger = logging.getLogger('raw_md_refresh')
    logger.setLevel(logging.INFO)
    lfh = logging.FileHandler('./raw_md_refresh.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    lfh.setFormatter(formatter)
    logger.addHandler(lfh)

    # create a list of fits files within the directory of interest
    glob_strs = [os.path.join(args.path+"fields/*/*/*/*.fits*"),
                 os.path.join(args.path+"flats/*/*/*.fits*"),
                 os.path.join(args.path+"darks/*/*/*.fits*")]

    files_to_process = glob.glob(glob_strs[0])+glob.glob(glob_strs[1])+glob.glob(glob_strs[2])

    logger.info(f"Number of files to process: {len(files_to_process)}")
    if len(files_to_process) < 1:
        logger.warning("No files found for processing.")
    else:
        with Pool(args.nproc) as pool:
            # tqdm is used to generate a progress bar for the multiprocessing job
            for _ in tqdm.tqdm(pool.imap_unordered(update_raw_datatable, files_to_process),
                               total=len(files_to_process)):
                pass
