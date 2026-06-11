#!/bin/bash

# Docker Compose runner for driver tests. Mirrors tests/sqllogic/run_in_docker.sh.
#
# Usage:
#   ./run_in_docker.sh                # default 'drivers' kind
#   TEST_KIND=drivers ./run_in_docker.sh

set -u

TEST_KIND=${1:-${TEST_KIND:-drivers}}

: "${BUILD_DIR:=build}"

if test -z "${DRIVERS_DIR:-}"; then
	DRIVERS_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
	export DRIVERS_DIR
fi

if ! test -f "$DRIVERS_DIR/run_in_docker.sh"; then
	echo "DRIVERS_DIR is undefined or invalid"
	exit 255
fi

if test -z "${WORKSPACE:-}"; then
	WORKSPACE=$(realpath "$DRIVERS_DIR/../../")
	export WORKSPACE
fi

if ! test -f "$WORKSPACE/docker.env"; then
	touch "$WORKSPACE/docker.env"
fi

mkdir -p "$WORKSPACE/out/logs" "$WORKSPACE/out/drivers-tests"

if test -z "${BUILD_IMAGE:-}"; then
	BUILD_IMAGE=serenedb/serenedb-build-ubuntu:latest
	export BUILD_IMAGE
fi

DOCKER_UID="$(id -u)"
DOCKER_GID="$(id -g)"
DOCKER_SOCK_GID="$(stat -c '%g' /var/run/docker.sock 2>/dev/null || echo 999)"
export DOCKER_UID DOCKER_GID DOCKER_SOCK_GID

cd "$DRIVERS_DIR"

export BUILD_DIR

PREFIX="$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom 2>/dev/null | head -c 4)"

COMPOSE_FILE="docker-compose.${TEST_KIND}.yml"
if ! test -f "$DRIVERS_DIR/$COMPOSE_FILE"; then
	echo "Error: Unknown test kind '$TEST_KIND' - file '$COMPOSE_FILE' not found" >&2
	exit 255
fi

# Reap orphans from earlier interrupted runs before bringing the stack up.
# See tests/scripts/reap_stale_docker_orphans.sh for the full rationale.
source "$DRIVERS_DIR/../scripts/reap_stale_docker_orphans.sh"
reap_stale_docker_orphans

cleanup() {
	docker compose -p "${PREFIX}" -f "$COMPOSE_FILE" down --volumes --remove-orphans
}
trap cleanup EXIT INT TERM

docker compose \
	-p "${PREFIX}" \
	-f "$COMPOSE_FILE" \
	up \
	--attach tests \
	--exit-code-from tests \
	--remove-orphans

test_exit_code=$?

if ! test "${test_exit_code}" -eq "0"; then
	echo "$TEST_KIND tests failed!"
	for svc in $(docker compose -p "${PREFIX}" -f "$COMPOSE_FILE" ps -a --format '{{.Service}}' 2>/dev/null); do
		[[ "$svc" == "tests" ]] && continue
		echo "===== logs: $svc ====="
		docker compose -p "${PREFIX}" -f "$COMPOSE_FILE" logs "$svc" || true
	done
fi

exit "$test_exit_code"
