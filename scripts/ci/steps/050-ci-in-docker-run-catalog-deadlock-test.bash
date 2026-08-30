#!/bin/bash

# Concurrent multi-entry catalog commits must not deadlock the server.
#
# Runs against a serened of its own rather than the shared sqllogic stack: the
# failure this looks for takes the whole process down, so a regression here must
# not take every other suite with it.

set -o pipefail

cd "${WORKSPACE}" || exit 1

PORT="${CATALOG_DEADLOCK_PORT:-7899}"
DATA_DIR="$(mktemp -d)"
LOG_DIR="./out/logs"
mkdir -p "${LOG_DIR}"

"${BUILD_DIR}/bin/serened" "${DATA_DIR}" --listen="postgres://0.0.0.0:${PORT}" \
	>"${LOG_DIR}/catalog-deadlock-serened.log" 2>&1 &
server_pid=$!

cleanup() {
	kill -9 "${server_pid}" 2>/dev/null
	wait "${server_pid}" 2>/dev/null
	rm -rf "${DATA_DIR}"
}
trap cleanup EXIT

ready=0
for _ in $(seq 1 90); do
	if ! kill -0 "${server_pid}" 2>/dev/null; then
		break
	fi
	if timeout 5 psql -h 127.0.0.1 -p "${PORT}" -U postgres -d postgres -tAc "SELECT 1;" \
		>/dev/null 2>&1; then
		ready=1
		break
	fi
	sleep 1
done

if [[ ${ready} -ne 1 ]]; then
	echo "serened did not come up on port ${PORT}" >&2
	tail -30 "${LOG_DIR}/catalog-deadlock-serened.log" >&2
	echo "CATALOG_DEADLOCK_TEST=FAILED"
	exit 123
fi

if ./tests/scripts/catalog_commit_deadlock.sh --port "${PORT}" \
	--workers "${CATALOG_DEADLOCK_WORKERS:-8}" \
	--seconds "${CATALOG_DEADLOCK_SECONDS:-40}" 2>&1 |
	tee -a "${LOG_DIR}/catalog-deadlock.log"; then
	test_result="PASSED"
	exit_code=0
else
	test_result="FAILED"
	exit_code=123
	# A wedged server keeps its stacks; the log carries whatever it reported,
	# including ThreadSanitizer's lock-order inversions on a tsan config.
	tail -60 "${LOG_DIR}/catalog-deadlock-serened.log" >&2
fi

echo "CATALOG_DEADLOCK_TEST=${test_result}"
exit ${exit_code}
