""" *Minimal* wrappers around LSST command line tasks.
Eventually we should stop using these and call LSST functions directly.
"""
import os

from lsst.pipe.tasks.ingest import IngestTask
from lsst.utils import getPackageDir

from huntsman.drp.lsst.utils.task import run_cmdline_task, run_cmdline_task_subprocess
from huntsman.drp.lsst.tasks.ingestRefcat import HuntsmanIngestIndexedReferenceTask
from huntsman.drp.lsst.tasks.processCcd import HuntsmanProcessCcdTask


INGEST_CALIB_CONFIGS = {"bias": "ingestBias.py",
                        "dark": "ingestDark.py",
                        "flat": "ingestFlat.py"}


MASTER_CALIB_SCRIPTS = {"bias": "constructBias.py",
                        "dark": "constructDark.py",
                        "flat": "constructFlat.py"}


def ingest_raw_data(filenames, butler_dir, mode="link", ignore_ingested=True):
    """ Ingest raw files into a butler repository.
    Args:
        filenames (list of str): The list of filenames to ingest.
        bulter_directory (str): The path to the butler directory.
        mode (str): The mode with which to store files. Can be "copy", "move" or "link".
            Default is "link".
        ignore_ingested (bool): If True (default), no error is raised if the same dataId is
            attempted to be ingested twice. In this case, the duplicate file is ignored.
    """
    # Create the ingest task
    task = IngestTask()
    task = task.prepareTask(root=butler_dir, mode=mode, ignoreIngested=ignore_ingested)

    # Ingest the files
    task.ingestFiles(filenames)


def ingest_reference_catalogue(butler_dir, filenames, output_directory=None):
    """Ingest a photometric reference catalogue (currently skymapper).
    Args:
        butler_dir (str): Directory that contains the butler repo.
    filenames (list of str): List of reference catalogue files to ingest.
    output_directory (str, optional): Directory that contains the output data reposity,
        by default None.
    """
    if output_directory is None:
        output_directory = butler_dir

    # Load the config file
    pkgdir = getPackageDir("obs_huntsman")
    config_file = os.path.join(pkgdir, "config", "ingestSkyMapperReference.py")
    config = HuntsmanIngestIndexedReferenceTask.ConfigClass()
    config.load(config_file)

    # Convert the files into the correct format and place them into the repository
    args = [butler_dir,
            "--configfile", config_file,
            "--output", output_directory,
            "--clobber-config",
            *filenames]
    HuntsmanIngestIndexedReferenceTask.parseAndRun(args=args)


def ingest_master_calibs(datasetType, filenames, butler_dir, calib_dir, validity):
    """ Ingest the master calib of a given date.
    Args:
        datasetType (str): One of bias, dark or flat.
        filenames (list of str): List of filenames to ingest.
        butler_dir (str): Directory that contains the butler repo.
        calib_dir (str): Directory that contains the calib repo.
        validity (int): Validity period in days for calib files.
    """
    cmd = f"ingestCalibs.py {butler_dir}"
    cmd += " " + " ".join(filenames)
    cmd += f" --validity {validity}"
    cmd += f" --calib {calib_dir} --mode=link"

    # We currently have to provide the config explicitly
    config_file = INGEST_CALIB_CONFIGS[datasetType]

    config_file = os.path.join(getPackageDir("obs_huntsman"), "config", config_file)
    cmd += " --config clobber=True"
    cmd += f" --configfile {config_file}"

    # Run the LSST command
    run_cmdline_task_subprocess(cmd)


def make_master_calib(datasetType, calibId, dataIds, butler_dir, calib_dir, rerun, nodes=1,
                      procs=1):
    """ Use the LSST stack to create a single master calib given a calibId and set of dataIds.
    Args:
        datasetType (str): The calib datasetType (bias, dark, flat).
        calibId (dict): The calibId.
        datIds (list of dict): The list of dataIds used to produce the master calibs.
        butler_dir (str): The path to the butler repository.
        calib_dir (str): The path to the butler calib repository.
        rerun (str): The rerun name.
        nodes (int, optional): The number of nodes to use, by default 1.
        procs (int, optional): The number of procs to use, by default 1.
    Returns:
        subprocess.CompletedProcess: The completed subprocess used to run the LSST command.
    """
    # Make the command to run the LSST task
    cmd = f"{MASTER_CALIB_SCRIPTS[datasetType]} {butler_dir} --rerun {rerun}"
    cmd += f" --calib {calib_dir}"
    for data_id in dataIds:
        cmd += " --id"
        for k, v in data_id.items():
            cmd += f" {k}={v}"
    cmd += " --calibId " + " ".join([f"{k}={v}" for k, v in calibId.items()])
    cmd += f" --nodes {nodes} --procs {procs}"
    cmd += " --doraise"  # We want the code to raise an error if there is a problem

    # Run the LSST script
    return run_cmdline_task_subprocess(cmd)


def make_calexp(dataId, rerun, butler_dir, calib_dir, doReturnResults=True, **kwargs):
    """ Make calibrated exposures (calexps) using the LSST stack.
    Args:
        dataId (abc.Mapping): The data ID of the science frame to process.
        rerun (str): The name of the rerun.
        butler_dir (str): The butler repository directory name.
        calib_dir (str): The calib directory used by the butler repository.
        doReturnResults (bool): If True (default), return results from LSST task.
        **kwargs: Parsed to make_calexps.
    Returns:
        dict or None: The result of HuntsmanProcessCcdTask.
    """
    return make_calexps([dataId], rerun, butler_dir, calib_dir, doReturnResults=doReturnResults,
                        **kwargs)


def make_calexps(dataIds, rerun, butler_dir, calib_dir, procs=1, clobber_config=False,
                 doReturnResults=False, extra_config=None, **kwargs):
    """ Make calibrated exposures (calexps) using the LSST stack.
    Args:
        dataIds (list of abc.Mapping): The data IDs of the science frames to process.
        rerun (str): The name of the rerun.
        butler_dir (str): The butler repository directory name.
        calib_dir (str): The calib directory used by the butler repository.
        procs (int, optional): The number of processes to use per node, by default 1.
        clobber_config (bool, optional): Override config values, by default False.
        doReturnResults (bool): If True, return results from LSST task. Default: False.
        extra_config (dict, optional): Extra config items for the LSST task.
        **kwargs: Parsed to run_cmdline_task.
    Returns:
        dict or None: The result of HuntsmanProcessCcdTask.
    """
    cmd = f"{butler_dir}"
    cmd += f" --rerun {rerun}"
    cmd += f" --calib {calib_dir}"
    cmd += f" -j {procs}"
    if clobber_config:
        cmd += " --clobber-config"

    for dataId in dataIds:
        cmd += " --id"
        for k, v in dataId.items():
            cmd += f" {k}={v}"

    extra_config = {} if extra_config is None else extra_config
    if extra_config:
        cmd += " --config"
        for k, v in extra_config.items():
            cmd += f" {k}={v}"

    result = run_cmdline_task(HuntsmanProcessCcdTask, cmd.split(), doReturnResults=doReturnResults,
                              **kwargs)

    if doReturnResults:
        return result.resultList[0].result.getDict()

    return result


def make_discrete_sky_map(butler_dir, calib_dir, rerun):
    """Create a sky map that covers processed exposures.
    Args:
        butler_dir (str): The butler directory.
        calib_dir (str): The calib directory.
        rerun (str): The rerun name.
    """
    cmd = f"makeDiscreteSkyMap.py {butler_dir} --calib {calib_dir} --id --rerun {rerun}"
    run_cmdline_task_subprocess(cmd)


def make_coadd_temp_exp(butler_dir, calib_dir, rerun, tract_id, patch_ids, filter_name):
    """ Warp exposures onto the skymap.
    Args:
        butler_dir (str): The butler directory.
        calib_dir (str): The calib directory.
        rerun (str): The rerun name.
        tract_id (int): The tract ID.
        patch_ids (list): A list of patch indices (x, y indices).
        filter_name (str): The filter name.
    """
    cmd = f"makeCoaddTempExp.py {butler_dir} --calib {calib_dir} --rerun {rerun}"
    cmd += f" --selectId filter={filter_name}"
    cmd += f" --id filter={filter_name}"
    cmd += f" tract={tract_id}"
    cmd += " patch=" + "^".join(patch_ids)
    run_cmdline_task_subprocess(cmd)


def assemble_coadd(butler_dir, calib_dir, rerun, tract_id, patch_ids, filter_name):
    """ Assemble the coadd from warped exposures.
    Args:
        butler_dir (str): The butler directory.
        rerun (str): The rerun name.
        tract_id (int): The tract ID.
        patch_ids (list): A list of patch indices (x, y indices).
        filter_name (str): The filter name.
    """
    cmd = f"assembleCoadd.py {butler_dir} --calib {calib_dir} --rerun {rerun}"
    cmd += f" --selectId filter={filter_name}"
    cmd += f" --id filter={filter_name}"
    cmd += f" tract={tract_id}"
    cmd += " patch=" + "^".join(patch_ids)
    run_cmdline_task_subprocess(cmd)
