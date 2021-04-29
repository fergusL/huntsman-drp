#!/usr/bin/env bash
set -e

source ${HUNTSMAN_DRP}/docker/bash-config.sh
python ${HUNTSMAN_DRP}/scripts/pyro/start-pyro-services.py
