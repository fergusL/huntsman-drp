#!/usr/bin/env bash
set -e

source ${HUNTSMAN_DRP}/docker/bash-config.sh
python ${HUNTSMAN_DRP}/scripts/calib/run-calib-maker.py
