#!/bin/bash
# This script is run from outside of the docker environment
set -eu

HUNTSMAN_DRP_COVDIR=${HUNTSMAN_DRP_COVDIR:-${HUNTSMAN_DRP}/coverage}
COMPOSE_FILE=${HUNTSMAN_DRP}/docker/testing/docker-compose.yml

function cleanup {
  echo "Stopping docker testing services."
  docker-compose -f ${COMPOSE_FILE} down
}

mkdir -p ${HUNTSMAN_DRP_COVDIR} && chmod -R 777 ${HUNTSMAN_DRP_COVDIR}

echo "Building new docker image for testing..."
docker-compose -f ${COMPOSE_FILE} build python-tests

echo "Running python tests inside docker container..."
trap cleanup EXIT
docker-compose -f ${COMPOSE_FILE} run --rm \
  -e "HUNTSMAN_COVERAGE=/opt/lsst/software/stack/coverage" \
  -v "${HUNTSMAN_DRP_COVDIR}:/opt/lsst/software/stack/coverage" \
  python-tests
