import os
from huntsman.drp.calib import MasterCalibMaker

if __name__ == "__main__":

    monitor = MasterCalibMaker()

    # Set niceness level
    niceness = monitor.config.get("niceness", None)
    if niceness:
        os.nice(niceness - os.nice(0))

    # Run the calib maker
    monitor.start()
