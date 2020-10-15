"""Script to plate solve all fits/fits.fz files in a given directory, add wcs infomation to the headers.
"""
import os
import argparse
from multiprocessing import Pool
from functools import partial
import logging

from panoptes.utils.images.fits import get_solve_field, fpack
from panoptes.utils.error import Timeout
import tqdm


def solve_field_task(options, fpath):
    """Small wrapper function to run get_solve_field() and ignore any timeouts.

    Args:
        options (dict): dictionary containing one keyword 'solve_opts' which maps
                        to a list of solve-field arguments
        fpath (str): The filepath of the file to be processed by solve-field
    """
    logger = logging.getLogger('main-log')
    debug_logger = logging.getLogger('debug-log')
    logger.info(f"Processing {fpath}")
    # frist check if the file exists
    if not os.path.isfile(fpath):
        logger.warning(f"File does not exist, {fpath}")
        return
    # now check if the file needs to be decompressed
    if fpath.endswith('.fz'):
        # try to decomprsess it using funpack
        try:
            fpath = fpack(fpath, unpack=True, overwrite=True)
        except Exception as e:
            # if an exception occurs, log the error
            debug_logger.warning(f"funpack failed on {fpath}, error: {e}")
        # Just incase the previous except doesn't capture an fpack error
        if not os.path.isfile(fpath):
            # if the expected output file does not exists, log the fpack failure
            logger.warning(f"funpack failed on {fpath}")
            return
        else:
            logger.info(f"Uncompressed file, output fpath is {fpath}")
    logger.info(f"Solving astrometry for {fpath}")
    # attempt to run solve-field on file
    try:
        get_solve_field(fpath, **options)
    # is attempt times out, log the info
    except Timeout as e:
        debug_logger.warning(
            f"get_solve_field() Timed out on {fpath} with error: \n{e}\n")
    # add another generic except to catch/log any unexpected errors
    except Exception as e:
        debug_logger.warning(f"get_solve_field() failed with error: \n{e}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, default="./",
                        help="Directory to search for files for processing.")
    parser.add_argument('--timeout', type=int, default=30,
                        help="The time out length (S) for each solve-field job.")
    parser.add_argument('--downsample', type=int, default=10,
                        help="Downsample images by an integer factor, makes processing faster")
    parser.add_argument('--scalelow', type=int, default=1,
                        help="Set minimum scale (deg) for solve-field to look for features.")
    parser.add_argument('--scalehigh', type=int, default=2,
                        help="Set maximum scale (deg) for solve-field to look for features.")
    parser.add_argument('--nproc', type=int, default=1,
                        help="Number of processors to use in multiprocessing.")
    args = parser.parse_args()

    # create a list of fits files within the directory of interest
    fpaths = []
    for dirpath, dirnames, filenames in os.walk(args.path):
        for file in filenames:
            # append the filepath to fpaths if file is a fits or fits.fz file
            if file.endswith('.fits') or file.endswith('.fits.fz'):
                fpaths.append(os.path.join(dirpath, file))

    logger = logging.getLogger('main-log')
    logger.setLevel(logging.INFO)
    lfh = logging.FileHandler(os.path.join(args.path, 'astrometry.log'))
    logger.addHandler(lfh)

    debug_logger = logging.getLogger('debug-log')
    debug_logger.setLevel(logging.DEBUG)
    dlfh = logging.FileHandler(os.path.join(args.path, 'debug_astrometry.log'))
    debug_logger.addHandler(dlfh)

    logger.info(f'Number of files to solve: {len(fpaths)}')

    options = {'solve_opts':
               [
                   '--guess-scale',
                   '--cpulimit', str(args.timeout),
                   '--no-verify',
                   '--crpix-center',
                   '--temp-axy',
                   '--index-xyls', 'none',
                   '--solved', 'none',
                   '--match', 'none',
                   '--rdls', 'none',
                   '--corr', 'none',
                   '--downsample', str(args.downsample),
                   '--scale-units', "degwidth",
                   '--scale-low', str(args.scalelow),
                   '--scale-high', str(args.scalehigh),
                   '--no-plots',
               ]
               }

    logger.info(f"{options}")

    # Calculate wcs for files
    logger.info(f"Processing astrometry for {len(fpaths)} files.")
    with Pool(args.nproc) as pool:
        work = partial(solve_field_task, options)
        # tqdm is used to generate a progress bar for the multiprocessing job
        for _ in tqdm.tqdm(pool.imap_unordered(work, fpaths), total=len(fpaths)):
            pass
