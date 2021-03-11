#!/bin/bash
# This script is intended to be run from inside a docker container. Please use run_local_tests
# to test software locally.
source ~/.bashrc
set -eu

# Put coverage files somewhere we have permissions to write them. ${LSST_HOME} by default.
COVERAGE_ROOT=${HUNTSMAN_COVERAGE:-${LSST_HOME}}
cd ${COVERAGE_ROOT}

pytest ${HUNTSMAN_DRP} -xv \
  --cov=huntsman.drp \
  --cov-config=${HUNTSMAN_DRP}/src/huntsman/drp/.coveragerc \
  --cov-report html:${COVERAGE_ROOT}/coverage.html \
  --cov-report xml:${COVERAGE_ROOT}/coverage.xml \
  --session2file=${COVERAGE_ROOT}/pytest_session.txt

exit 0
