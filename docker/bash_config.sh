# Setup the LSST bash env
source ${LSST_HOME}/loadLSST.bash
# Use the EUPS package manager to install modules
eups declare obs_huntsman v1 -r "${LSST_HOME}/obs_huntsman"
eups declare huntsman_drp v1 -r "${LSST_HOME}/huntsman-drp"
setup obs_huntsman v1
setup huntsman_drp v1
setup display_firefly
# ---- Below this line are temporary requirements for astrometry.net ----
alias python=python3
# need this so panoptes-utils knows where to find solve-field executable
export SOLVE_FIELD=$(which solve-field)
