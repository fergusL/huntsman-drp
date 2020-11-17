"""
Script to run through all raw files and populate the DQ database with metadata.
"""
import os
import argparse
from functools import partial
from multiprocessing import Pool

from huntsman.drp.datatable import RawDataTable, RawQualityTable
from huntsman.drp.quality import metadata_from_fits
from huntsman.drp.core import get_logger


def initialise_pool(niceness):
    """ Set the niceness level of the pool processes.
    Args:
        niceness (int): The niceness level.
    """
    if niceness is not None:
        n = os.nice(0)
        os.nice(niceness - n)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--nprocs", type=int, default=1,
                        help="The number of processes to run on.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Only run this many files."
                             " If 0 (default), run all the files.")
    parser.add_argument("--niceness", type=int, default=5, help="The niceness level.")

    args = parser.parse_args()
    nprocs = args.nprocs
    limit = args.limit
    niceness = args.niceness

    logger = get_logger()

    # Get filenames of raw data
    rawdatatable = RawDataTable()
    filenames = rawdatatable.query()["filename"].values
    if limit != 0:
        logger.info(f"Limiting to processing to {limit} files.")
        filenames = filenames[:limit]

    # Get quality metadata for files
    fn = partial(metadata_from_fits, logger=logger)
    logger.info(f"Getting quality metadata for {len(filenames)} files.")
    with Pool(nprocs, initializer=initialise_pool, initargs=(niceness,)) as pool:
        metadata_list = pool.map(fn, filenames)

    # Update metadata in table
    logger.info(f"Adding quality metadata to database.")
    dqtable = RawQualityTable()
    dqtable.update(metadata_list, upsert=True)
