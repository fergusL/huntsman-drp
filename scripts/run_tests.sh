#!/bin/bash
source ~/.bashrc

set -eu

COVERAGE_REPORT_HTML=${HUNTSMAN_DRP}/htmlcov
COVERAGE_REPORT_XML=${HUNTSMAN_DRP}/coverage.xml

cd ${HUNTSMAN_DRP}/src/huntsman/drp

pytest -x --cov=huntsman.drp --cov-report html:${COVERAGE_REPORT_HTML} \
  --cov-report xml:${COVERAGE_REPORT_XML} --cov-config=.coveragerc

exit 0
