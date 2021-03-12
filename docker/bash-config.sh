# Setup the LSST bash env
source ${LSST_HOME}/loadLSST.bash

# Use the EUPS package manager to set up required packages
eups declare obs_huntsman v1 -r "${LSST_HOME}/obs_huntsman"
eups declare huntsman_drp v1 -r "${LSST_HOME}/huntsman-drp"
setup obs_huntsman v1
setup huntsman_drp v1
setup display_firefly

# This seems to be the only way of getting the logs to work properly
userid=$(id -u ${USER})
sudo chown ${userid}:${userid} ${HUNTSMAN_LOG_DIR}
