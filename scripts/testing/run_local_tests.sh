#!/bin/bash
source ~/.bashrc
set -eu

# Put coverage files somewhere we have permissions to write them. ${LSST_HOME} by default.
COVERAGE_ROOT=${HUNTSMAN_COVERAGE:-${LSST_HOME}}
cd ${COVERAGE_ROOT}

pytest ${HUNTSMAN_DRP} -x \
  --cov=huntsman.drp \
  --cov-config=${HUNTSMAN_DRP}/src/huntsman/drp/.coveragerc \
  --cov-report xml:${COVERAGE_ROOT}/coverage.xml \
  --session2file=${COVERAGE_ROOT}/pytest_session.txt

exit 0
