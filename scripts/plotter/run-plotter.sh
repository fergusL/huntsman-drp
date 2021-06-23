#!/usr/bin/env bash
set -e

source ${HUNTSMAN_DRP}/docker/ingestor/bash-config.sh
python ${HUNTSMAN_DRP}/scripts/plotter/run-plotter.py
