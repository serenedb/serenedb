#!/bin/bash

# Docker Compose runner for sqllogic tests
# Usage:
#   ./run_in_docker.sh                    # Uses default 'sqllogic' kind
#   ./run_in_docker.sh recovery           # Uses 'recovery' kind
#   TEST_KIND=recovery ./run_in_docker.sh # Uses env variable

# - Think about run_* scripts renaming. Or even rewrite into python???


# Determine test kind
TEST_KIND=${1:-${TEST_KIND:-sqllogic}}

# Map test kind to compose file
COMPOSE_FILE="docker-compose.$TEST_KIND.yml"

if test -z "$SQLLOGIC_DIR"; then
  export SQLLOGIC_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
fi

if ! test -f "$SQLLOGIC_DIR/run_in_docker.sh"; then
    echo "SQLLOGIC_DIR is undefined or invalid"
    exit 255
fi

# Validate that compose file exists
if ! test -f "$SQLLOGIC_DIR/$COMPOSE_FILE"; then
    echo "Error: Unknown test kind '$TEST_KIND' - file '$COMPOSE_FILE' not found" >&2
    exit 255
fi

# In case of running locally
if test -z "$WORKSPACE"; then
    export WORKSPACE=$(realpath "$SQLLOGIC_DIR/../../")
fi

if ! test -f "$WORKSPACE/docker.env"; then
    touch "$WORKSPACE/docker.env"
fi

# Docker container assumes that sanitizers and coverage dirs are created.
# Executable 'serened' switches to 'serenedb' user. If required dirs are not created,
# /serenedb dir owner is 'root' and it's impossible for 'serenedb' user to create directory there.
mkdir -p "$WORKSPACE/sanitizers"
mkdir -p "$WORKSPACE/build/coverage"

if test -z "$BUILD_IMAGE"; then
    export BUILD_IMAGE=10.serenedb.com:5000/serenedb-build-ubuntu:latest
fi

cd $SQLLOGIC_DIR

# can be useful to run from container: docker compose run tests bash
docker compose -f "$COMPOSE_FILE" up --attach tests --exit-code-from tests --remove-orphans
test_exit_code=$?
if ! test "${test_exit_code}" -eq "0"; then
  echo "$TEST_KIND tests failed!"
  echo "serenedb-single container log begin:"
  docker compose -f "$COMPOSE_FILE" logs serenedb-single
  echo "serenedb-single container log end!"
fi
docker compose -f "$COMPOSE_FILE" down --volumes
exit "$test_exit_code"
