"""Script to monitor for new DWF run exposures and calibrate/coadd them.

-assume cal data taken at some other time

-monitor mongodb raw metadata database etc monitor it for new files

-recheck for new files every "exposure time length" query by timestamp
(look datatable)

-get files, ingest and do the processing (processCcd then follow tutorial
for doing coadds.... also need skymapper catalogue for region of interest
(look in refcat))

-will have a mounted output directory in the docker container, make this a
parameter output_directory
"""
from lsst.utils import getPackageDir

from dateutil.parser import parse as parse_date
import datetime.datetime as datetime
import argparse
import subprocess
import os


def ingest_master_bias(date, bias_config_file, datadir='DATA',
                       calibdir='DATA/CALIB', rerun='processCcdOutputs',
                       validity=1000):
    """Ingest the master bias of a given date."""

    print(f"Ingesting master bias frames.")
    cmd = f"ingestCalibs.py {datadir}"
    cmd += f" {datadir}/rerun/{rerun}/calib/bias/{date}/*/*.fits"
    cmd += f" --validity {validity}"
    cmd += f" --calib {calibdir} --mode=link"
    cmd += " --config clobber=True"
    cmd += f" --configfile {bias_config_file}"
    print(f'The ingest command is: {cmd}')
    subprocess.call(cmd, shell=True)


def ingest_master_flat(date, filter, flat_config_file, datadir='DATA',
                       calibdir='DATA/CALIB', rerun='processCcdOutputs',
                       validity=1000):
    """Ingest the master flat of a given date."""
    print(f"Ingesting master {filter} filter flats frames.")
    cmd = f"ingestCalibs.py {datadir}"
    cmd += f" {datadir}/rerun/{rerun}/calib/flat/{date}/*/*.fits"
    cmd += f" --validity {validity}"
    cmd += f" --calib {calibdir} --mode=link"
    cmd += " --config clobber=True"
    cmd += f" --configfile {flat_config_file}"
    print(f'The ingest command is: {cmd}')
    subprocess.call(cmd, shell=True)


def ingest_sci_images(file_list, datadir='DATA', calibdir='DATA/CALIB'):
    """Ingest science images to be processed."""
    cmd = f"ingestImages.py {datadir}"
    cmd += f" testdata/science/*.fits --mode=link --calib {calibdir}"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)


def processCcd(dataType='science', datadir='DATA', calibdir='DATA/CALIB',
               rerun='processCcdOutputs'):
    """Process ingested exposures."""
    cmd = f"processCcd.py {datadir} --rerun {rerun}"
    cmd += f" --calib {calibdir} --id dataType={dataType}"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)


def makeDiscreteSkyMap(datadir='DATA', rerun='processCcdOutputs:coadd'):
    """Create a sky map that covers processed exposures."""
    cmd = f"makeDiscreteSkyMap.py {datadir} --id --rerun {rerun} "
    cmd += f"--config skyMap.projection='TAN'"
    subprocess.call(cmd, shell=True)


def makeCoaddTempExp(filter, datadir='DATA', calibdir='DATA/CALIB',
                     rerun='coadd'):
    """Warp exposures onto sky map."""
    cmd = f"makeCoaddTempExp.py {datadir} --rerun {rerun} "
    cmd += f"--selectId filter={filter} --id filter={filter} tract=0 "
    cmd += f"patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2 "
    cmd += f"--config doApplyUberCal=False"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)


def assembleCoadd(filter, datadir='DATA', calibdir='DATA/CALIB',
                  rerun='coadd'):
    """Assemble the warped exposures into a coadd"""
    cmd = f"assembleCoadd.py {datadir} --rerun {rerun} "
    cmd += f"--selectId filter={filter} --id filter={filter} tract=0 "
    cmd += f"patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)


def process_concurrent_exposures(file_list, master_bias_date, master_flat_date,
                                 datadir='DATA', calibdir='DATA/CALIB',
                                 validity=1000):
    """Function that takes list of science exposures and processes them to
    produce a coadd. Master calibs are assumed to have already been produced
    amd ingested into butler repo. Skymapper catalogue is also assumed to have
    been ingested.

    Args:
        files (list): List of filepaths for processing.

    TODO:
        -Find way to handle exposures with different filters
    """
    flat_config_file = os.path.join(getPackageDir("obs_huntsman"), "config",
                                    "ingestFlats.py")

    bias_config_file = os.path.join(getPackageDir("obs_huntsman"), "config",
                                    "ingestBiases.py")

    ingest_master_bias(master_bias_date, bias_config_file, datadir='DATA',
                       calibdir='DATA/CALIB', rerun='processCcdOutputs',
                       validity=1000)

    ingest_master_flat(master_flat_date, filter, flat_config_file,
                       datadir='DATA', calibdir='DATA/CALIB',
                       rerun='processCcdOutputs', validity=1000)

    ingest_sci_images(file_list, datadir='DATA', calibdir='DATA/CALIB')

    processCcd(dataType='science', datadir='DATA', calibdir='DATA/CALIB',
               rerun='processCcdOutputs')

    makeDiscreteSkyMap(datadir='DATA', rerun='processCcdOutputs:coadd')

    makeCoaddTempExp(filter, datadir='DATA', calibdir='DATA/CALIB',
                     rerun='coadd')

    assembleCoadd(filter, datadir='DATA', calibdir='DATA/CALIB',
                  rerun='coadd')


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('abc', type=str, help='abc.')
    # args = parser.parse_args()
    pass
    # process_concurrent_exposures(files)
