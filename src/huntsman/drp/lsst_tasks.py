import os
import subprocess

from lsst.pipe.tasks.ingest import IngestTask
from lsst.utils import getPackageDir

# from lsst.pipe.drivers.constructCalibs import BiasTask, FlatTask
from huntsman.drp.utils import date_to_ymd


def ingest_raw_data(filename_list, butler_directory, mode="link", ignore_ingested=False):
    """

    """
    # Create the ingest task
    task = IngestTask()
    task = task.prepareTask(root=butler_directory, mode=mode, ignoreIngested=ignore_ingested)

    # Ingest the files
    task.ingestFiles(filename_list)


def constructBias(calib_date, exptime, ccd, butler_directory, calib_directory, rerun, data_ids,
                  nodes=1, procs=1):
    """

    """
    calib_date = date_to_ymd(calib_date)
    cmd = f"constructBias.py {butler_directory} --rerun {rerun}"
    cmd += f" --calib {calib_directory}"
    cmd += f" --id visit={'^'.join([f'{id}' for id in data_ids])}"
    cmd += " dataType='bias'"
    cmd += f" expTime={exptime}"
    cmd += f" ccd={ccd}"
    cmd += f" --nodes {nodes} --procs {procs}"
    cmd += f" --calibId expTime={exptime} calibDate={calib_date}"
    subprocess.call(cmd, shell=True)


def constructFlat(calib_date, filter_name, ccd, butler_directory, calib_directory, rerun, data_ids,
                  nodes=1, procs=1):
    """

    """
    calib_date = date_to_ymd(calib_date)
    cmd = f"constructFlat.py {butler_directory} --rerun {rerun}"
    cmd += f" --calib {calib_directory}"
    cmd += f" --id visit={'^'.join([f'{id}' for id in data_ids])}"
    cmd += " dataType='flat'"
    cmd += f" filter={filter_name}"
    cmd += f" --nodes {nodes} --procs {procs}"
    cmd += f" --calibId filter={filter_name} calibDate={calib_date}"
    subprocess.call(cmd, shell=True)


def ingest_master_biases(calib_date, butler_directory, calib_directory, rerun, validity=1000):
    """
    Ingest the master bias of a given date.
    """
    calib_date = date_to_ymd(calib_date)
    cmd = f"ingestCalibs.py {butler_directory}"
    # TODO - Remove hard-coded directory structure
    cmd += f" {butler_directory}/rerun/{rerun}/calib/bias/{calib_date}/*/*.fits"
    cmd += f" --validity {validity}"
    cmd += f" --calib {calib_directory} --mode=link"

    # For some reason we have to provide the config explicitly
    config_file = os.path.join(getPackageDir("obs_huntsman"), "config", "ingestBiases.py")
    cmd += " --config clobber=True"
    cmd += f" --configfile {config_file}"

    subprocess.call(cmd, shell=True)


def ingest_master_flat(calib_date, butler_directory, calib_directory, rerun, validity=1000):
    """
    Ingest the master flat of a given date.
    """
    calib_date = date_to_ymd(calib_date)
    cmd = f"ingestCalibs.py {butler_directory}"
    # TODO - Remove hard-coded directory structure
    cmd += f" {butler_directory}/rerun/{rerun}/calib/flat/{calib_date}/*/*.fits"
    cmd += f" --validity {validity}"
    cmd += f" --calib {calib_directory} --mode=link"

    # For some reason we have to provide the config explicitly
    config_file = os.path.join(getPackageDir("obs_huntsman"), "config", "ingestFlats.py")
    cmd += " --config clobber=True"
    cmd += f" --configfile {config_file}"

    subprocess.call(cmd, shell=True)


def ingest_sci_images(file_list, butler_directory='DATA', calib_directory='DATA/CALIB'):
    """Ingest science images to be processed."""
    cmd = f"ingestImages.py {butler_directory}"
    cmd += f" testdata/science/*.fits --mode=link --calib {calib_directory}"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)


def processCcd(dataType='science', butler_directory='DATA', calib_directory='DATA/CALIB',
               rerun='processCcdOutputs'):
    """Process ingested exposures."""
    cmd = f"processCcd.py {butler_directory} --rerun {rerun}"
    cmd += f" --calib {calib_directory} --id dataType={dataType}"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)


def makeDiscreteSkyMap(butler_directory='DATA', rerun='processCcdOutputs:coadd'):
    """Create a sky map that covers processed exposures."""
    cmd = f"makeDiscreteSkyMap.py {butler_directory} --id --rerun {rerun} "
    cmd += f"--config skyMap.projection='TAN'"
    subprocess.call(cmd, shell=True)


def makeCoaddTempExp(filter, butler_directory='DATA', calib_directory='DATA/CALIB',
                     rerun='coadd'):
    """Warp exposures onto sky map."""
    cmd = f"makeCoaddTempExp.py {butler_directory} --rerun {rerun} "
    cmd += f"--selectId filter={filter} --id filter={filter} tract=0 "
    cmd += f"patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2 "
    cmd += f"--config doApplyUberCal=False"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)


def assembleCoadd(filter, butler_directory='DATA', calib_directory='DATA/CALIB',
                  rerun='coadd'):
    """Assemble the warped exposures into a coadd"""
    cmd = f"assembleCoadd.py {butler_directory} --rerun {rerun} "
    cmd += f"--selectId filter={filter} --id filter={filter} tract=0 "
    cmd += f"patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)
