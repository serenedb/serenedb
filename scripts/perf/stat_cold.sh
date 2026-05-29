#!/usr/bin/env bash
# Like profile_cold.sh but uses `perf stat` instead of `perf record`.
# Captures page-faults / major-faults / task-clock / cpu-clock per cold
# query so we can compare cs vs native I/O wait directly.
#
# Per query: kill serened, drop OS cache, restart, re-attach native_db,
# perf stat the cold query, dump counters.
#
# Pre-reqs: passwordless `sudo -v` cached; perf installed.
# Inputs:
#   PERF_BUILD_DIR        default ../../build_perf
#   PERF_SERENED_DATA_DIR default <RESULTS>/types_perf_data
#   PERF_NATIVE_DB        default <RESULTS>/types_perf_native.duckdb
#   PERF_PORT             default 6263
#   QUERIES               default "f64"

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
SERENED_DATA_DIR="${PERF_SERENED_DATA_DIR:-${RESULTS_DIR}/types_perf_data}"
NATIVE_DB="${PERF_NATIVE_DB:-${RESULTS_DIR}/types_perf_native.duckdb}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
PORT="${PERF_PORT:-6263}"
LOG="/tmp/${USER}-serened-stat-cold.log"
PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"
QUERIES="${QUERIES:-f64}"
RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)"

declare -A EXPR=(
	[count]="COUNT(*)"
	[i8]="SUM(i8::BIGINT)"
	[i16]="SUM(i16::BIGINT)"
	[i32]="SUM(i32::BIGINT)"
	[i64]="SUM(i64)"
	[f32]="SUM(f32::DOUBLE)"
	[f64]="SUM(f64)"
	[varchar]="SUM(length(s))"
	[bool]="SUM(CASE WHEN bool_col THEN 1 ELSE 0 END)"
	[boolCast]="SUM(bool_col::INTEGER)"
	[boolFilt]="COUNT(*) FILTER (WHERE bool_col)"
	[struct]="SUM(struct_basic.a) + SUM(length(struct_basic.b))"
	[structF]="SUM(struct_f64.a) + SUM(length(struct_f64.b))"
	[array]="SUM(arr_i32[1] + arr_i32[2] + arr_i32[3])"
	[arrayF]="SUM(arr_f64[1] + arr_f64[2] + arr_f64[3])"
	[list]="SUM(list_sum(lst_i32))"
	[map]="SUM(list_sum(map_values(map_i32)))"
	[lstStr]="SUM(list_sum(list_transform(lst_struct, p -> p.v)))"
	[deep]="SUM(length(deep.name)) + SUM(list_sum(list_transform(deep.vals, p -> p.v)))"
)

NDB_SQL_PATH=$(printf '%s' "${NATIVE_DB}" | sed "s/'/''/g")

start_server() {
	killall -9 serened >/dev/null 2>&1 || true
	for _ in $(seq 1 30); do
		pgrep -f "${SERENED_BIN}" >/dev/null || break
		sleep 0.2
	done
	"${SERENED_BIN}" "${SERENED_DATA_DIR}" \
		--server.endpoint "pgsql+tcp://0.0.0.0:${PORT}" \
		--log.foreground-tty true \
		>"${LOG}" 2>&1 &
	disown
	for _ in $(seq 1 60); do
		psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1 && return 0
		sleep 0.5
	done
	echo "serened did not come up" >&2
	tail -50 "${LOG}" >&2
	exit 1
}

reattach() {
	psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X >/dev/null <<EOF
ATTACH IF NOT EXISTS '${NDB_SQL_PATH}' AS native_db (TYPE duckdb, STORAGE_VERSION latest);
SET search_path TO public, native_db.main;
EOF
}

drop_caches() {
	sync
	sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null
}

stat_cold() {
	local label="$1" table="$2" tag="$3"
	local expr="${EXPR[${label}]:-}"
	[[ -z "${expr}" ]] && {
		echo "unknown query: ${label}" >&2
		exit 1
	}

	killall -9 serened >/dev/null 2>&1 || true
	for _ in $(seq 1 30); do
		pgrep -f "${SERENED_BIN}" >/dev/null || break
		sleep 0.2
	done
	drop_caches
	start_server
	reattach

	local pid
	pid=$(pgrep -f "${SERENED_BIN}" | head -1)
	local out="${RESULTS_DIR}/stat-cold-${RUN_TAG}-${label}-${tag}.txt"

	echo
	echo "=== stat ${label} (${tag}) -- pid ${pid} ==="
	# perf stat attaches to the pid; we run the query in parallel and
	# kill perf when the query finishes. perf stat prints counters on
	# SIGINT.
	perf stat --pid="${pid}" --inherit \
		-e task-clock,cpu-clock,page-faults,minor-faults,major-faults,context-switches \
		-o "${out}" -- sleep 30 &
	local perf_pid=$!
	sleep 0.2

	psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X <<EOF
SET search_path TO public, native_db.main;
SET threads = 1;
\timing on
SELECT ${expr} FROM ${table};
EOF

	kill -INT "${perf_pid}" 2>/dev/null || true
	wait "${perf_pid}" 2>/dev/null || true

	echo "--- counters ${label} (${tag}) ---"
	cat "${out}"
}

echo "caching sudo credential for drop_caches"
sudo -v || {
	echo "ERROR: sudo -v failed" >&2
	exit 1
}
(
	while kill -0 "$$" 2>/dev/null; do
		sudo -n true 2>/dev/null || break
		sleep 30
	done
) &
SUDO_REFRESH_PID=$!
trap "kill ${SUDO_REFRESH_PID} 2>/dev/null; killall -9 serened >/dev/null 2>&1 || true" EXIT

for q in ${QUERIES}; do
	stat_cold "${q}" "bench_idx" "cs"
	stat_cold "${q}" "bench_native" "native"
done

killall -9 serened >/dev/null 2>&1 || true
echo
echo "=== reports ==="
ls -1 "${RESULTS_DIR}"/stat-cold-"${RUN_TAG}"-*.txt
