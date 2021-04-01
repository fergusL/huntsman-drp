#!/usr/bin/env bash
set -e

source ${HUNTSMAN_DRP}/docker/bash-config.sh
python ${HUNTSMAN_DRP}/scripts/run-calib-maker.py
