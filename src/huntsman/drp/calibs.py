from dateutil.parser import parse as parse_date
from huntsman.drp.meta import MetaDatabase
from datetime import datetime
import lsst.daf.persistence as dafPersist
from lsst.pipe.drivers.constructCalibs import BiasTask, FlatTask
from lsst.pipe.tasks.ingestCalibs import IngestCalibsTask
from lsst.utils import getPackageDir


def constructHuntsmanBiases(data_dir,
                            min_num_exposures,
                            datadir='DATA',
                            calibdir='DATA/CALIB',
                            rerun='processCcdOutputs',
                            nodes=1,
                            procs=1,
                            validity=1000):
    """Construct biases.

    Parameters
    ----------
    data_dir : str
        Directory containing the biases to be processed, the butler instance
        is initiated within this directory.
    min_num_exposures : int
        Minimum number of bias exposures required to proceed with construction
        of bias.
    datadir : str
        Directory containing the LSST butler database.
    calibdir : str
        Directory containing LSST butler calibration files.
    rerun : str
        The LSST rerun chain to be used in constructing biases.
    nodes : int
        Number of nodes to be used, default of 1.
    procs : int
        Number of CPUs per node.
    validity : int
        Calibration validity period in days.

    Returns
    -------
    type
        Description of returned object.

    """
    # get the ingestbias config
    config_file = os.path.join(getPackageDir("obs_huntsman"), "config",
                               "ingestBiases.py")
    # Create the Bulter object
    butler = dafPersist.Butler(inputs=os.path.join(os.environ['LSST_HOME'],
                                                   datadir))
    # Query butler for dark exposures
    # TODO: Replace visit with imageId
    metalist = butler.queryMetadata('raw',
                                    ['ccd', 'expTime', 'dateObs', 'visit'],
                                    dataId={'dataType': 'bias'})

    # Select the exposures we are interested in
    exposures = defaultdict(dict)
    for (ccd, exptime, dateobs, imageId) in metalist:

        Reject exposures outside of date range
        dateobs = parse_date(dateobs)
        if (dateobs < date_start) or (dateobs > date_end):
            continue

        # Update the list of calibs we need
        if exptime not in exposures[ccd].keys():
            exposures[ccd][exptime] = []
        exposures[ccd][exptime].append(imageId)

    # Create the master calibs if we have enough data
    for ccd, exptimes in exposures.items():
        for exptime, image_ids in exptimes.items():

            n_exposures = len(image_ids)

            if n_exposures < min_num_exposures:
                print(f'Not enough exposures for {exptime}s biases on ccd'
                      f' {ccd} ({n_exposures} of {min_exposures}).')
                continue

            print(f'Making master biases for ccd {ccd} using {n_exposures}'
                  f' exposures of {exptime}s.')

            # Construct the calib for this ccd/exptime combination
            # TODO: Replace visit with imageId
            # TODO: create function that can generate these shell cmd strings?
            cmd = f"constructBias.py {datadir} --rerun {rerun}"
            cmd += f" --calib {calibdir}"
            cmd += f" --id visit={'^'.join([f'{id}' for id in image_ids])}"
            cmd += f" expTime={exptime}"
            cmd += f" --nodes {nodes} --procs {procs}"
            cmd += f" --calibId expTime={exptime} calibDate={date}"
            print(f'The command is: {cmd}')
            # subprocess.call(cmd, shell=True)
            # rather than running as a subprocess, split cmd by spaces and
            # supply the resulting list of strings to `parseAndRun`
            BiasTask.parseAndRun(cmd.split()[1:])

    # Ingest the master calibs
    # TODO: Lookup the correct directory
    # TODO: maybe this should be a separate function?
    print(f"Ingesting master bias frames.")
    cmd = f"ingestCalibs.py {datadir}"
    cmd += f" {datadir}/rerun/{rerun}/calib/bias/{date}/*/*.fits"
    cmd += f" --validity {validity}"
    cmd += f" --calib {calibdir} --mode=link"
    cmd += " --config clobber=True"
    cmd += f" --configfile {config_file}"
    print(f'The ingest command is: {cmd}')
    # subprocess.call(cmd, shell=True)
    # rather than running as a subprocess, split cmd by spaces and
    # supply the resulting list of strings to `parseAndRun`
    IngestCalibsTask.parseAndRun(cmd.split()[1:])
    # TODO: alert/log message when subprocess completes


def constructHuntsmanFlats(data_dir,
                           min_num_exposures,
                           datadir='DATA',
                           calibdir='DATA/CALIB',
                           rerun='processCcdOutputs',
                           nodes=1,
                           procs=1,
                           validity=1000):
    """Construct flats.

    Parameters
    ----------
    data_dir : str
        Directory containing the biases to be processed, the butler instance
        is initiated within this directory.
    min_num_exposures : int
        Minimum number of flat exposures required to proceed with construction
        of flat.
    datadir : str
        Directory containing the LSST butler database.
    calibdir : str
        Directory containing LSST butler calibration files.
    rerun : str
        The LSST rerun chain to be used in constructing biases.
    nodes : int
        Number of nodes to be used, default of 1.
    procs : int
        Number of CPUs per node.
    validity : int
        Calibration validity period in days.
    """
    config_file = os.path.join(getPackageDir("obs_huntsman"), "config",
                               "ingestFlats.py")
    # Create the Bulter object
    butler = dafPersist.Butler(inputs=os.path.join(os.environ['LSST_HOME'],
                                                   datadir))
    # Query butler for dark exposures
    # TODO: Replace visit with expId
    metalist = butler.queryMetadata('raw',
                                    ['ccd', 'filter', 'dateObs', 'expId'],
                                    dataId={'dataType': 'flat'})

    # Select the exposures we are interested in
    exposures = defaultdict(dict)
    for (ccd, filter, dateobs, expId) in metalist:

        # Reject exposures outside of date range
        dateobs = parse_date(dateobs)
        if (dateobs < date_start) or (dateobs > date_end):
            continue

        # Update the list of calibs we need
        if filter not in exposures[ccd].keys():
            exposures[ccd][filter] = []
        exposures[ccd][filter].append(expId)

    # Create the master calibs if we have enough data
    for ccd, filters in exposures.items():
        for filter, exp_ids in filters.items():

            n_exposures = len(exp_ids)

            if n_exposures < min_exposures:
                print(f'Not enough exposures for flats in {filter} filter on'
                      f' ccd {ccd} ({n_exposures} of {min_exposures}).')
                continue

            print(f'Making master flats for ccd {ccd} using {n_exposures}'
                  f' exposures in {filter} filter.')

            # Construct the calib for this ccd/exptime combination
            cmd = f"constructFlat.py {datadir} --rerun {rerun}"
            cmd += f" --calib {calibdir}"
            cmd += f" --id expId={'^'.join([f'{id}' for id in exp_ids])}"
            cmd += " dataType='flat'"  # TODO: remove
            cmd += f" filter={filter}"
            cmd += f" --nodes {nodes} --procs {procs}"
            cmd += f" --calibId filter={filter} calibDate={date}"
            print(f'The command is: {cmd}')
            # subprocess.call(cmd, shell=True)
            # rather than running as a subprocess, split cmd by spaces and
            # supply the resulting list of strings to `parseAndRun`
            FlatTask.parseAndRun(cmd.split()[1:])

    # Ingest the master calibs
    # TODO: Lookup the correct directory
    # TODO: maybe this should be a separate function?
    print(f"Ingesting master {filter} filter flats frames for ccd {ccd}.")
    cmd = f"ingestCalibs.py {datadir}"
    cmd += f" {datadir}/rerun/{rerun}/calib/flat/{date}/*/*.fits"
    cmd += f" --validity {validity}"
    cmd += f" --calib {calibdir} --mode=link"
    cmd += " --config clobber=True"
    cmd += f" --configfile {config_file}"
    print(f'The ingest command is: {cmd}')
    # subprocess.call(cmd, shell=True)
    # rather than running as a subprocess, split cmd by spaces and
    # supply the resulting list of strings to `parseAndRun`
    IngestCalibsTask.parseAndRun(cmd.split()[1:])
    # TODO: alert/log message when subprocess completes


def make_recent_calibs(butler_directory,
                       date=datetime.today().strftime('%Y-%m-%d'),
                       min_num_exposures=10,
                       date_range=7,
                       **kwargs):
    """
    Takes a date and constructs calibrations using files within a range of
    specified date. Then saves the calib files to specified output directory.
    Parameters
    ----------
    butler_directory : str
        Directory containing the LSST butler database.
    date : type str
        Date specified in form yyyy-mm-dd, defaults to todays date.
    min_num_exposures : int
        Minimum numver of files needed to produce a master calib.
    date_range : int
        Number of days either side of specified date to search for useable
        calib data.
    """
    # create a MetaDatabase instance
    db = MetaDatabase()
    # retrieve desired files and place in a tmp directory
    # TODO something to setup tmp directories
    date_parsed = parse_date(date)
    date_range = datetime.timedelta(days=date_range)

    db.retreive_files(output_dir,
                      date_min=date_parsed - date_range,
                      date_max=date_parsed - date_range)
    constructHuntsmanBiases(butler_directory, min_num_exposures, **kwargs)
    constructHuntsmanFlats(butler_directory, min_num_exposures, **kwargs)
