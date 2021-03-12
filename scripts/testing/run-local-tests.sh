#!/bin/bash
# This script is run from outside of the docker environment
# To run without building new docker images, use --no-build
set -eu
ARG1=${1:-"--build"}

COMPOSE_FILE=${HUNTSMAN_DRP}/docker/testing/docker-compose.yml
export HUNTSMAN_LOG_DIR=${HUNTSMAN_LOG_DIR:-${HUNTSMAN_DRP}/logs}

function cleanup {
  echo "Stopping docker testing services."
  docker-compose -f ${COMPOSE_FILE} down
}

echo "Local log directory: ${HUNTSMAN_LOG_DIR}"
mkdir -p ${HUNTSMAN_LOG_DIR} && chmod -R 777 ${HUNTSMAN_LOG_DIR}

if [ ${ARG1} != "--no-build" ]; then
  echo "Building new docker image(s) for testing..."
  docker-compose -f ${COMPOSE_FILE} build python-tests
fi

echo "Running python tests inside docker container..."
trap cleanup EXIT
docker-compose -f ${COMPOSE_FILE} run --rm python-tests
