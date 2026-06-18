#!/bin/bash
set -eo pipefail

# Tests the tarball: extract, start serened, run sqllogic tests.
# Expects: WORKSPACE, BUILD_IMAGE

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CI_DIR="$(dirname "$SCRIPT_DIR")"

TARBALL="${TARBALL:-$(ls "${WORKSPACE}"/serenedb-*.tar.gz 2>/dev/null | head -1)}"
if [[ -z "$TARBALL" ]]; then
	echo "ERROR: No tarball found in ${WORKSPACE}"
	exit 1
fi

echo "=== Tarball RTA: $(basename "$TARBALL") ==="
mkdir -p "${WORKSPACE}/out/logs"

export TARBALL_NAME="$(basename "$TARBALL")"
export DOCKER_UID="$(id -u)"
export DOCKER_GID="$(id -g)"
export CARGO_TARGET_CACHE="${CARGO_TARGET_CACHE:-${HOME}/.cache/serenedb-cargo-target}"
export CARGO_HOME_CACHE="${CARGO_HOME_CACHE:-${HOME}/.cache/serenedb-cargo-home}"
mkdir -p "$CARGO_TARGET_CACHE" "$CARGO_HOME_CACHE"

PREFIX="tarball-rta-$$"
COMPOSE_FILE="${CI_DIR}/docker-compose.tarball-rta.yml"

cleanup() {
	docker compose -p "$PREFIX" -f "$COMPOSE_FILE" logs serenedb 2>&1 >"${WORKSPACE}/out/logs/tarball-rta-serened.log" || true
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

# Optional drivers RTA. Gated on RTA_DRIVERS for the same reason as the
# .deb path.
if [[ $test_rc -eq 0 && "${RTA_DRIVERS:-false}" == "true" ]]; then
	echo "=== Drivers RTA (python+java) ==="
	DRIVERS_COMPOSE="${CI_DIR}/docker-compose.tarball-drivers-rta.yml"
	docker compose -p "${PREFIX}-drv" -f "$DRIVERS_COMPOSE" \
		up --attach tests --exit-code-from tests --remove-orphans \
		2>&1 | tee "${WORKSPACE}/out/logs/tarball-rta-drivers.log" || test_rc=$?
	docker compose -p "${PREFIX}-drv" -f "$DRIVERS_COMPOSE" \
		down --volumes --remove-orphans >/dev/null 2>&1 || true
fi

echo "TARBALL_RTA=$([ $test_rc -eq 0 ] && echo PASSED || echo FAILED)"
exit $test_rc
