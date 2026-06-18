#!/bin/bash
set -eo pipefail

# Tests the production Docker image: start, sqllogic tests, verify logs.
# Expects: WORKSPACE, BUILD_IMAGE, DOCKER_TEST_IMAGE

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CI_DIR="$(dirname "$SCRIPT_DIR")"

if [[ -z "${DOCKER_TEST_IMAGE}" ]]; then
	echo "ERROR: DOCKER_TEST_IMAGE not set"
	exit 1
fi

echo "=== Docker RTA: ${DOCKER_TEST_IMAGE} ==="
mkdir -p "${WORKSPACE}/out/logs"

export DOCKER_UID="$(id -u)"
export DOCKER_GID="$(id -g)"
export CARGO_TARGET_CACHE="${CARGO_TARGET_CACHE:-${HOME}/.cache/serenedb-cargo-target}"

PREFIX="docker-rta-$$"
COMPOSE_FILE="${CI_DIR}/docker-compose.docker-rta.yml"

cleanup() {
	docker compose -p "$PREFIX" -f "$COMPOSE_FILE" logs serenedb 2>&1 >"${WORKSPACE}/out/logs/docker-rta-serened.log" || true
	docker compose -p "$PREFIX" -f "$COMPOSE_FILE" down --volumes --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT INT TERM

test_rc=0
docker compose -p "$PREFIX" -f "$COMPOSE_FILE" up \
	--attach tests \
	--exit-code-from tests \
	--remove-orphans || test_rc=$?

# Verify serened produced logs
log_lines=$(docker compose -p "$PREFIX" -f "$COMPOSE_FILE" logs serenedb 2>&1 | wc -l)
if [[ "$log_lines" -eq 0 ]]; then
	echo "ERROR: No serened log output found - service likely failed to start"
	test_rc=1
fi

# Optional drivers RTA. Gated on RTA_DRIVERS for the same reason as
# the .deb / tarball paths.
if [[ $test_rc -eq 0 && "${RTA_DRIVERS:-false}" == "true" ]]; then
	echo "=== Drivers RTA (python+java) ==="
	DRIVERS_COMPOSE="${CI_DIR}/docker-compose.docker-drivers-rta.yml"
	docker compose -p "${PREFIX}-drv" -f "$DRIVERS_COMPOSE" \
		up --attach tests --exit-code-from tests --remove-orphans \
		2>&1 | tee "${WORKSPACE}/out/logs/docker-rta-drivers.log" || test_rc=$?
	docker compose -p "${PREFIX}-drv" -f "$DRIVERS_COMPOSE" \
		down --volumes --remove-orphans >/dev/null 2>&1 || true
fi

echo "DOCKER_RTA=$([ $test_rc -eq 0 ] && echo PASSED || echo FAILED)"
exit $test_rc
