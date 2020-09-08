#!/bin/bash
source ~/.bashrc

set -eu

cd ${HUNTSMAN_DRP}/src/huntsman/drp

pytest -x --cov=huntsman.drp --cov-report html:htmlcov  --cov-config=.coveragerc

exit 0
