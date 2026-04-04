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
mkdir -p "${WORKSPACE}/logs"

export DOCKER_UID="$(id -u)"
export DOCKER_GID="$(id -g)"
export CARGO_TARGET_CACHE="${CARGO_TARGET_CACHE:-${HOME}/.cache/serenedb-cargo-target}"

PREFIX="docker-rta-$$"
COMPOSE_FILE="${CI_DIR}/docker-compose.docker-rta.yml"

cleanup() {
	docker compose -p "$PREFIX" -f "$COMPOSE_FILE" logs serenedb 2>&1 >"${WORKSPACE}/logs/docker-rta-serened.log" || true
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
	echo "WARNING: No serened log output found"
fi

echo "DOCKER_RTA=$([ $test_rc -eq 0 ] && echo PASSED || echo FAILED)"
exit $test_rc
