#!/usr/bin/env bash
# Profile the cold (post-restart, drop_caches) path of one or more
# bench queries.  Per query: kill serened, drop OS cache, restart,
# re-attach native_db, perf record while the single cold query runs.
# No looping -- we want the actual first-touch IO + footer parse.
#
# Assumes scripts/perf/run_types_perf.sh has already been run at least
# once so the data dir + native db + parquet exist.
#
# Pre-reqs: passwordless `sudo -v` cached (the script calls sudo -v),
#           perf installed and paranoid permissive enough.
# Inputs:
#   PERF_BUILD_DIR (default ../../build_perf)
#   PERF_PORT      (default 6263)
#   PERF_DURATION  (default 10 -- max seconds for perf record window)
#   QUERIES        (default "f64" -- space-separated label list)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
SERENED_DATA_DIR="${PERF_SERENED_DATA_DIR:-${RESULTS_DIR}/types_perf_data}"
NATIVE_DB="${PERF_NATIVE_DB:-${RESULTS_DIR}/types_perf_native.duckdb}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
PORT="${PERF_PORT:-6263}"
LOG="/tmp/${USER}-serened-cold-profile.log"
PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"
DURATION="${PERF_DURATION:-10}"
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
		--server_endpoint "pgsql+tcp://0.0.0.0:${PORT}" \
		>"${LOG}" 2>&1 &
	disown
	for _ in $(seq 1 60); do
		psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1 && return 0
		sleep 0.5
	done
	echo "serened did not come up; tail of ${LOG}:" >&2
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

profile_cold() {
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
	local data="${RESULTS_DIR}/perf-cold-${RUN_TAG}-${label}-${tag}.data"
	local txt="${RESULTS_DIR}/perf-cold-${RUN_TAG}-${label}-${tag}.report"

	echo
	echo "=== cold profile ${label} (${tag}) -- pid ${pid} ==="
	# Start perf, then issue the single cold query, then stop perf when
	# the query completes (the perf-record sleep also caps the window).
	perf record -F 999 --pid="${pid}" --inherit --call-graph fp \
		-o "${data}" -- sleep "${DURATION}" >/dev/null 2>&1 &
	local perf_pid=$!
	# Tiny pause so perf is sampling before the query starts.
	sleep 0.2
	psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X >/dev/null <<EOF
SET search_path TO public, native_db.main;
SET threads = 1;
\timing on
SELECT ${expr} FROM ${table};
EOF
	# Stop perf -- it's still sleeping for the remainder of DURATION.
	kill -INT "${perf_pid}" 2>/dev/null || true
	wait "${perf_pid}" 2>/dev/null || true

	perf report --no-children -i "${data}" --stdio \
		--percent-limit 0.5 --sort overhead,dso,symbol 2>/dev/null |
		sed -n '/^#\s*Overhead/,/^# (Tip\|^$/p' >"${txt}"
	echo "wrote ${txt}"
}

# sudo cache the credential for drop_caches.
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
	profile_cold "${q}" "bench_idx" "cs"
	profile_cold "${q}" "bench_native" "native"
done

killall -9 serened >/dev/null 2>&1
echo
echo "=== reports ==="
ls -1 "${RESULTS_DIR}"/perf-cold-"${RUN_TAG}"-*.report
