#!/bin/bash
# This script is intended to be run from inside a docker container. Please use run_local_tests
# to test software locally.
source ~/.bashrc
set -eu

# Put coverage files somewhere we have permissions to write them
cd ${HUNTSMAN_LOG_DIR}

pytest ${HUNTSMAN_DRP} -xv \
  --cov=huntsman.drp \
  --cov-config=${HUNTSMAN_DRP}/src/huntsman/drp/.coveragerc \
  --cov-report html:${HUNTSMAN_LOG_DIR}/coverage.html \
  --cov-report xml:${HUNTSMAN_LOG_DIR}/coverage.xml \
  --session2file=${HUNTSMAN_LOG_DIR}/pytest_session.txt

exit 0
