#!/usr/bin/env bash
# Loop the bool query in hot state to amass samples for `perf annotate`
# on DefaultSelect.  No drop_caches between iterations -- we want the
# steady-state cost, not IO time.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
SERENED_DATA_DIR="${PERF_SERENED_DATA_DIR:-${RESULTS_DIR}/types_perf_data}"
NATIVE_DB="${PERF_NATIVE_DB:-${RESULTS_DIR}/types_perf_native.duckdb}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
PORT="${PERF_PORT:-6263}"
LOG="/tmp/${USER}-serened-bool-hot.log"
PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"
DURATION="${PERF_DURATION:-20}"
RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)"

NDB_SQL_PATH=$(printf '%s' "${NATIVE_DB}" | sed "s/'/''/g")
EXPR="SUM(CASE WHEN bool_col THEN 1 ELSE 0 END)"

killall -9 serened >/dev/null 2>&1 || true
for _ in $(seq 1 20); do
	pgrep -f "${SERENED_BIN}" >/dev/null || break
	sleep 0.2
done
"${SERENED_BIN}" "${SERENED_DATA_DIR}" \
	--server.endpoint "pgsql+tcp://0.0.0.0:${PORT}" \
	--log.foreground-tty true \
	>"${LOG}" 2>&1 &
disown
for _ in $(seq 1 60); do
	psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1 && break
	sleep 0.5
done
psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X >/dev/null <<EOF
ATTACH IF NOT EXISTS '${NDB_SQL_PATH}' AS native_db (TYPE duckdb, STORAGE_VERSION latest);
SET search_path TO public, native_db.main;
EOF

PID=$(pgrep -f "${SERENED_BIN}" | head -1)

run_loop() {
	local table="$1"
	{
		echo "SET search_path TO public, native_db.main;"
		echo "SET threads = 1;"
		echo "\\o /dev/null"
		yes "SELECT ${EXPR} FROM ${table};" | head -n 100000
	} | psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X >/dev/null 2>&1 &
	echo $!
}
kill_loop() {
	kill -9 "$1" 2>/dev/null || true
	pkill -9 -P "$1" 2>/dev/null || true
}

profile() {
	local table="$1" tag="$2"
	local data="${RESULTS_DIR}/perf-bool-hot-${RUN_TAG}-${tag}.data"
	# Warm up
	psql "${PSQL_CONN}" -c "SET threads=1;" \
		-c "SET search_path TO public, native_db.main;" \
		-c "SELECT ${EXPR} FROM ${table};" >/dev/null

	echo "=== profile bool ${tag} ${DURATION}s -- pid ${PID} ==="
	LOOP_PID=$(run_loop "${table}")
	sleep 0.5
	perf record -F 999 --pid="${PID}" --inherit --call-graph fp \
		-o "${data}" -- sleep "${DURATION}" >/dev/null 2>&1
	kill_loop "${LOOP_PID}"
	echo "wrote ${data}"
}

profile bench_idx cs
profile bench_native native

killall -9 serened >/dev/null 2>&1
echo
echo "Sample counts:"
for f in ${RESULTS_DIR}/perf-bool-hot-${RUN_TAG}-*.data; do
	n=$(perf script -i "$f" 2>/dev/null | grep -c "^\S")
	echo "  $(basename "$f"): $n samples"
done
echo
echo "data files: ${RESULTS_DIR}/perf-bool-hot-${RUN_TAG}-*.data"
echo "next: perf annotate -i <data> --symbol DefaultSelect --stdio --no-source"
