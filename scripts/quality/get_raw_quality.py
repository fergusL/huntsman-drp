"""
Script to run through all raw files and populate the DQ database with metadata.
"""
import argparse
from multiprocessing import Pool

from huntsman.drp.datatable import RawDataTable, RawQualityTable
from huntsman.drp.quality import metadata_from_fits


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--replace", action="store_true",
                        help="If provided, delete existing quality metadata.")
    parser.add_argument("--nprocs", type=int, default=1,
                        help="The number of processes to run on.")
    args = parser.parse_args()
    replace = args.replace
    nprocs = args.nprocs

    # Get filenames of raw data
    rawdatatable = RawDataTable()
    filenames = rawdatatable.query()["filename"].values

    # Get quality metadata for files
    with Pool(nprocs) as pool:
        metadata_list = pool.map(metadata_from_fits, filenames)

    # Update metadata in table
    dqtable = RawQualityTable()
    for filename, metadata in zip(filenames, metadata_list):
        if replace:
            dqtable.delete_file_data(filename)
        dqtable.update_file_data(filename, data=metadata)
