#!/bin/bash
source ~/.bashrc

set -eu

cd ${HUNTSMAN_DRP}/src/huntsman/drp
pytest -xv

exit 0
