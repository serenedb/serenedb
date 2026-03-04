#!/bin/bash

# Docker Compose runner for sqllogic tests
# Usage:
#   ./run_in_docker.sh                    # Uses default 'sqllogic' kind
#   ./run_in_docker.sh recovery           # Uses 'recovery' kind
#   TEST_KIND=recovery ./run_in_docker.sh # Uses env variable

TEST_KIND=${1:-${TEST_KIND:-sqllogic}}

if test -z "$SQLLOGIC_DIR"; then
  export SQLLOGIC_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
fi

if ! test -f "$SQLLOGIC_DIR/run_in_docker.sh"; then
    echo "SQLLOGIC_DIR is undefined or invalid"
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

if [[ "$TEST_KIND" == "recovery" ]]; then
  SERVICE_NAME="serenedb-recovery-serened"
  NETWORK_NAME="serenedb-recovery-net"
  VOLUME_NAME="serenedb-recovery-datadir"

  docker swarm init 2>/dev/null || true
  docker network create --driver overlay --attachable "$NETWORK_NAME" 2>/dev/null || true
  # Named volume persists across Swarm task restarts (unlike /tmp which is per-container)
  docker volume create "$VOLUME_NAME"

  docker service create \
    --name "$SERVICE_NAME" \
    --restart-condition on-failure \
    --replicas 1 \
    --restart-delay 0s \
    --restart-max-attempts 1 \
    --restart-window 1s \
    --network "$NETWORK_NAME" \
    --mount type=bind,src="$WORKSPACE",dst=/serenedb \
    --mount type=bind,src="$WORKSPACE/logs",dst=/var/log/serenedb \
    --mount type=volume,src="$VOLUME_NAME",dst=/serenedb_datadir \
    "${env_args[@]}" \
    "$BUILD_IMAGE" \
    sh -c '
      if ! id serenedb >/dev/null 2>&1; then
        useradd serenedb &&
        chown -R serenedb:serenedb /serenedb/sanitizers /serenedb/build/coverage
      fi &&
      chown serenedb:serenedb /serenedb_datadir &&
      exec /serenedb/build/bin/serened /serenedb_datadir \
        --server.endpoint pgsql+tcp://0.0.0.0:7777 \
        --server.endpoint tcp://0.0.0.0:8529 \
        --server.authentication 0
    '

  # Run tests
  docker run --rm \
    --network "$NETWORK_NAME" \
    -e "SERVICE_HOST=$SERVICE_NAME" \
    -v "$SQLLOGIC_DIR:/sqllogic" \
    -v "$WORKSPACE/third_party/sqllogictest-rs:/sqllogictest-rs" \
    rust:latest \
    bash -c 'exec sqllogic/run_recovery.sh'
  test_exit_code=$?

  if ! test "${test_exit_code}" -eq "0"; then
    echo "Recovery tests failed!"
    echo "serened service log begin:"
    docker service logs "$SERVICE_NAME"
    echo "serened service log end!"
  fi

  docker service rm "$SERVICE_NAME"
  docker network rm "$NETWORK_NAME"
  docker volume rm "$VOLUME_NAME"
else
  COMPOSE_FILE="docker-compose.$TEST_KIND.yml"
  # Validate that compose file exists
  if ! test -f "$SQLLOGIC_DIR/$COMPOSE_FILE"; then
      echo "Error: Unknown test kind '$TEST_KIND' - file '$COMPOSE_FILE' not found" >&2
      exit 255
  fi

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
fi

exit "$test_exit_code"
