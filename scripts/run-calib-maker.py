from huntsman.drp.calib import MasterCalibMaker

if __name__ == "__main__":

    nproc = 20  # TODO: Command line arg

    monitor = MasterCalibMaker(nproc=nproc)
    monitor.start()
