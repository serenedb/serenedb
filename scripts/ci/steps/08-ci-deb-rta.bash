#!/bin/bash
set -eo pipefail

# Tests the .deb package: install, configure, sqllogic tests, service lifecycle.
# Uses systemd container for serenedb + separate tests container (same as normal CI tests).
# Expects: WORKSPACE, BUILD_IMAGE

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CI_DIR="$(dirname "$SCRIPT_DIR")"

DEB_PACKAGE="${DEB_PACKAGE:-$(ls "${WORKSPACE}"/serenedb_*.deb 2>/dev/null | head -1)}"
if [[ -z "$DEB_PACKAGE" ]]; then
	echo "ERROR: No .deb package found in ${WORKSPACE}"
	exit 1
fi

echo "=== Deb RTA: $(basename "$DEB_PACKAGE") ==="
mkdir -p "${WORKSPACE}/out/logs"
mkdir -p "${WORKSPACE}/out/test-tmp"

# Export all env vars ONCE so compose sees consistent config across all calls
export DEB_PACKAGE="$(basename "$DEB_PACKAGE")"
export DOCKER_UID="$(id -u)"
export DOCKER_GID="$(id -g)"
export CARGO_TARGET_CACHE="${CARGO_TARGET_CACHE:-${HOME}/.cache/serenedb-cargo-target}"

PREFIX="deb-rta-$$"
COMPOSE_FILE="${CI_DIR}/docker-compose.deb-rta.yml"
EXEC="docker compose -p ${PREFIX} -f ${COMPOSE_FILE} exec -T serenedb"

cleanup() {
	$EXEC journalctl -u serenedb --no-pager 2>/dev/null >"${WORKSPACE}/out/logs/deb-rta-journal.log" || true
	docker compose -p "$PREFIX" -f "$COMPOSE_FILE" down --volumes --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Start systemd container
docker compose -p "$PREFIX" -f "$COMPOSE_FILE" up -d serenedb

# Setup: install .deb, configure, start service (via exec into running systemd container)
echo "=== Setup ==="
$EXEC /deb-rta-setup.sh

# Run sqllogic tests (separate container, same network)
echo "=== Sqllogic tests ==="
test_rc=0
docker compose -p "$PREFIX" -f "$COMPOSE_FILE" up \
	--no-deps --attach tests --exit-code-from tests \
	--remove-orphans tests || test_rc=$?

if [[ $test_rc -ne 0 ]]; then
	echo "DEB_RTA=FAILED (sqllogic tests)"
	exit $test_rc
fi

# Optional drivers RTA (python+java only). Gated on RTA_DRIVERS to keep
# normal RTA wall time bounded; turn on once D1 driver harness is stable.
if [[ "${RTA_DRIVERS:-false}" == "true" ]]; then
	echo "=== Drivers RTA (python+java) ==="
	DRIVERS_COMPOSE="${CI_DIR}/docker-compose.deb-drivers-rta.yml"
	drivers_rc=0
	docker compose -p "${PREFIX}-drv" -f "$DRIVERS_COMPOSE" \
		up --attach tests --exit-code-from tests --remove-orphans \
		2>&1 | tee "${WORKSPACE}/out/logs/deb-rta-drivers.log" || drivers_rc=$?
	docker compose -p "${PREFIX}-drv" -f "$DRIVERS_COMPOSE" \
		down --volumes --remove-orphans >/dev/null 2>&1 || true
	if [[ $drivers_rc -ne 0 ]]; then
		echo "DEB_RTA=FAILED (drivers)"
		exit $drivers_rc
	fi
fi

# Service lifecycle tests
check() {
	local desc="$1"
	shift
	if "$@"; then
		echo "OK: $desc"
	else
		echo "FAIL: $desc"
		exit 1
	fi
}

echo "=== Service stop ==="
$EXEC systemctl stop serenedb
check "service stopped" $EXEC bash -c '! systemctl is-active --quiet serenedb'

echo "=== Service restart ==="
$EXEC systemctl start serenedb
sleep 2
check "service restarted" $EXEC systemctl is-active --quiet serenedb

echo "=== Crash recovery ==="
$EXEC bash -c 'kill -9 $(pgrep serened)'
sleep 10
check "service recovered after crash" $EXEC systemctl is-active --quiet serenedb

echo "=== apt remove ==="
$EXEC apt-get remove -y serenedb
check "binary removed" $EXEC test ! -f /usr/bin/serened
check "data dir preserved after remove" $EXEC test -d /var/lib/serenedb

echo "=== apt purge ==="
$EXEC apt-get purge -y serenedb
check "data dir removed after purge" $EXEC test ! -d /var/lib/serenedb
check "log dir removed after purge" $EXEC test ! -d /var/log/serenedb

# Verify journal has logs (collected by cleanup trap)
log_lines=$($EXEC journalctl -u serenedb --no-pager 2>/dev/null | wc -l || echo 0)
if [[ "$log_lines" -eq 0 ]]; then
	echo "WARNING: No journal logs for serenedb"
fi

echo "DEB_RTA=PASSED"
