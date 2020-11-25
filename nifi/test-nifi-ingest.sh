#!/bin/zsh
# This is a simple script that will create the conda env and attempt to parse a FITS file.
# The script expects a single positional argument that is the filename of the FITS file.
set -eu
source ~/anaconda3/etc/profile.d/conda.sh

ENV_NAME="huntsman-drp-nifi-test"

# Always run the trap command before exiting
trap "conda deactivate && conda remove -y --name ${ENV_NAME} --all" EXIT

# Create the NiFi environment
conda remove -y --name ${ENV_NAME} --all
conda create -y --name ${ENV_NAME} python=3.6 pip
conda activate ${ENV_NAME}
pip install -r ${HUNTSMAN_DRP}/nifi/requirements.txt
cd ${HUNTSMAN_DRP} && pip install -e .

# Run the FITS parser script on the file
python ${HUNTSMAN_DRP}/nifi/parse_fits_metadata.py $1
