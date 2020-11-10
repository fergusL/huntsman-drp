import os
import copy
import subprocess

from lsst.pipe.tasks.ingest import IngestTask
from lsst.utils import getPackageDir

from lsst.meas.algorithms import IngestIndexedReferenceTask
# from lsst.pipe.drivers.constructCalibs import BiasTask, FlatTask

from huntsman.drp.utils.date import date_to_ymd
from huntsman.drp.utils.butler import get_unique_calib_ids, fill_calib_keys
from huntsman.drp.core import get_logger


def run_command(cmd, logger=None):
    if logger is None:
        logger = get_logger()
    logger.debug(f"Running LSST command in subprocess: {cmd}")
    return subprocess.check_output(cmd, shell=True)


def ingest_raw_data(filename_list, butler_directory, mode="link", ignore_ingested=False):
    """

    """
    # Create the ingest task
    task = IngestTask()
    task = task.prepareTask(root=butler_directory, mode=mode, ignoreIngested=ignore_ingested)

    # Ingest the files
    task.ingestFiles(filename_list)


def ingest_reference_catalogue(butler_directory, filenames, output_directory=None):
    """

    """
    if output_directory is None:
        output_directory = butler_directory

    # Load the config file
    pkgdir = getPackageDir("obs_huntsman")
    config_file = os.path.join(pkgdir, "config", "ingestSkyMapperReference.py")
    config = IngestIndexedReferenceTask.ConfigClass()
    config.load(config_file)

    # Convert the files into the correct format and place them into the repository
    args = [butler_directory,
            "--configfile", config_file,
            "--output", output_directory,
            "--clobber-config",
            *filenames]
    IngestIndexedReferenceTask.parseAndRun(args=args)


def ingest_master_calibs(datasetType, filenames, butler_directory, calib_directory, validity):
    """
    Ingest the master bias of a given date.
    """
    cmd = f"ingestCalibs.py {butler_directory}"
    cmd += " " + " ".join(filenames)
    cmd += f" --validity {validity}"
    cmd += f" --calib {calib_directory} --mode=link"

    # For some reason we have to provide the config explicitly
    if datasetType == "bias":
        config_file = "ingestBiases.py"
    elif datasetType == "flat":
        config_file = "ingestFlats.py"
    else:
        raise ValueError(f"Unrecognised calib datasetType: {datasetType}.")
    config_file = os.path.join(getPackageDir("obs_huntsman"), "config", config_file)
    cmd += " --config clobber=True"
    cmd += f" --configfile {config_file}"

    # Run the LSST command
    run_command(cmd)


def make_master_calibs(datasetType, data_ids, calib_date, butler_repository, rerun, nodes=1,
                       procs=1):
    """
    Use constructBias.py to construct master bias frames for the data_ids. The master calibs are
    produced for each unique calibId obtainable from the list of dataIds.

    Args:
        datasetType (str): The calib datasetType (e.g. bias, flat).
        data_ids (list of dict): The list of dataIds used to produce the master calibs.
        calib_date (date): The date to associate with the master calibs.
        butler_repository (huntsman.drp.butler.ButlerRepository): The butler repository object.
        rerun (str): The rerun name.
        nodes (int): The number of nodes to run on.
        procs (int): The number of processes to use per node.
    """
    calib_date = date_to_ymd(calib_date)

    if datasetType == "bias":
        script_name = "constructBias.py"
    elif datasetType == "flat":
        script_name = "constructFlat.py"
    else:
        raise ValueError(f"Unrecognised calib datasetType: {datasetType}.")

    # Prepare the dataIds
    data_ids = copy.deepcopy(data_ids)
    for data_id in data_ids:
        # Fill required missing keys
        data_id.update(fill_calib_keys(data_id, datasetType, butler=butler_repository.butler,
                                       keys_ignore=["calibDate"]))
        # Add the calib date to the dataId
        data_id["calibDate"] = calib_date

    # For some reason we have to run each calibId separately
    unique_calib_ids = get_unique_calib_ids(datasetType, data_ids, butler=butler_repository.butler)
    for calib_id in unique_calib_ids:

        # Get data_ids corresponding to this calib_id
        data_id_subset = [d for d in data_ids if calib_id.items() <= d.items()]

        # Construct the command
        cmd = f"{script_name} {butler_repository.butler_directory} --rerun {rerun}"
        cmd += f" --calib {butler_repository.calib_directory}"
        for data_id in data_id_subset:
            cmd += " --id"
            for k, v in data_id.items():
                cmd += f" {k}={v}"
        cmd += f" --nodes {nodes} --procs {procs}"
        cmd += f" --calibId " + " ".join([f"{k}={v}" for k, v in calib_id.items()])

        # Run the LSST command
        run_command(cmd)


def processCcd(butler_directory, calib_directory, rerun, filter_name, dataType='science'):
    """Process ingested exposures."""
    cmd = f"processCcd.py {butler_directory} --rerun {rerun}"
    cmd += f" --id dataType={dataType} filter={filter_name}"
    cmd += f" --calib {calib_directory}"
    subprocess.check_output(cmd, shell=True)


def makeDiscreteSkyMap(butler_directory='DATA', rerun='processCcdOutputs:coadd'):
    """Create a sky map that covers processed exposures."""
    cmd = f"makeDiscreteSkyMap.py {butler_directory} --id --rerun {rerun} "
    cmd += f"--config skyMap.projection='TAN'"
    subprocess.check_output(cmd, shell=True)


def makeCoaddTempExp(filter, butler_directory='DATA', calib_directory='DATA/CALIB',
                     rerun='coadd'):
    """Warp exposures onto sky map."""
    cmd = f"makeCoaddTempExp.py {butler_directory} --rerun {rerun} "
    cmd += f"--selectId filter={filter} --id filter={filter} tract=0 "
    cmd += f"patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2 "
    cmd += f"--config doApplyUberCal=False"
    print(f'The command is: {cmd}')
    subprocess.check_output(cmd, shell=True)


def assembleCoadd(filter, butler_directory='DATA', calib_directory='DATA/CALIB',
                  rerun='coadd'):
    """Assemble the warped exposures into a coadd"""
    cmd = f"assembleCoadd.py {butler_directory} --rerun {rerun} "
    cmd += f"--selectId filter={filter} --id filter={filter} tract=0 "
    cmd += f"patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2"
    print(f'The command is: {cmd}')
    subprocess.check_output(cmd, shell=True)
