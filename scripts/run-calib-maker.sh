#!/usr/bin/env bash
set -e

source ${HUNTSMAN_DRP}/docker/bash-config.sh
${HUNTSMAN_DRP}/scripts/run-service huntsman.drp.services.calib.MasterCalibMaker
