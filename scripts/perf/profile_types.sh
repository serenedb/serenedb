#!/usr/bin/env bash
# Perf-record one slow query at a time, separately for the cs side and
# the duckdb-native side, against the data dir / native db already laid
# down by scripts/perf/run_types_perf.sh.  Lets us compare which
# symbols dominate on each side -- structural delta surfaces here.
#
# Pre-req: passwordless `sudo perf` (or run as root).
# Inputs:
#   PERF_BUILD_DIR (default ../../build_perf)
#   PERF_SERENED_DATA_DIR (default ../../build_perf_types_data)
#   PERF_NATIVE_DB (default ../../build_perf_types.duckdb)
#   PERF_PORT (default 6263)
#   PERF_DURATION (default 20 -- seconds per profile)
#   QUERIES  (default "i32 varchar struct" -- space-separated label list)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
SERENED_DATA_DIR="${PERF_SERENED_DATA_DIR:-${RESULTS_DIR}/types_perf_data}"
NATIVE_DB="${PERF_NATIVE_DB:-${RESULTS_DIR}/types_perf_native.duckdb}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
PORT="${PERF_PORT:-6263}"
LOG="/tmp/${USER}-serened-profile.log"
PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"
RESULTS_DIR="${ROOT}/scripts/perf/results"
DURATION="${PERF_DURATION:-20}"
QUERIES_DEFAULT="i32 varchar struct"
QUERIES="${QUERIES:-${QUERIES_DEFAULT}}"

mkdir -p "${RESULTS_DIR}"
RUN_TAG="$(date -u +%Y%m%dT%H%M%SZ)"

declare -A EXPR=(
	[i8]="SUM(i8::BIGINT)"
	[i16]="SUM(i16::BIGINT)"
	[i32]="SUM(i32::BIGINT)"
	[i64]="SUM(i64)"
	[f32]="SUM(f32::DOUBLE)"
	[f64]="SUM(f64)"
	[varchar]="SUM(length(s))"
	[bool]="SUM(CASE WHEN bool_col THEN 1 ELSE 0 END)"
	[struct]="SUM(struct_basic.a) + SUM(length(struct_basic.b))"
	[array]="SUM(arr_i32[1] + arr_i32[2] + arr_i32[3])"
	[list]="SUM(list_sum(lst_i32))"
	[map]="SUM(list_sum(map_values(map_i32)))"
	[lstStr]="SUM(list_sum(list_transform(lst_struct, p -> p.v)))"
	[deep]="SUM(length(deep.name)) + SUM(list_sum(list_transform(deep.vals, p -> p.v)))"
)

start_server() {
	killall -9 serened >/dev/null 2>&1 || true
	sleep 1
	"${SERENED_BIN}" "${SERENED_DATA_DIR}" \
		--server_endpoints "pgsql+tcp://0.0.0.0:${PORT}" \
		>"${LOG}" 2>&1 &
	disown
	for _ in $(seq 1 30); do
		psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1 && return 0
		sleep 0.5
	done
	echo "serened did not come up; tail of ${LOG}:" >&2
	tail -50 "${LOG}" >&2
	exit 1
}

setup_session() {
	# Re-issue CREATE INDEX (view-backed indexes lose their SQL alias on
	# restart; the catalog row + .cs files survive, but the alias bind is
	# session-local).  CREATE INDEX is safe because iresearch dedups
	# against existing .cs files for the segments already on disk.
	psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 <<EOF
ATTACH IF NOT EXISTS '${NATIVE_DB}' AS native_db (TYPE duckdb, STORAGE_VERSION latest);
SET search_path TO public, native_db.main;
CREATE INDEX IF NOT EXISTS bench_idx ON bench_view USING inverted()
INCLUDE (
  i8, i16, i32, i64, f32, f64, s, bool_col,
  arr_i32, lst_i32, struct_basic, map_i32, lst_struct, deep
);
EOF
}

# Queue a generous batch of identical SELECTs in one psql session.
# perf record runs its own DURATION-second clock; once it stops, we
# kill the workload.  Need enough queries to outlast perf -- generate
# 1M SELECT lines (covers seconds-of-work even for the fastest types).
run_loop() {
	local table="$1" expr="$2"
	{
		echo "SET search_path TO public, native_db.main;"
		echo "SET threads = 1;"
		echo "\\o /dev/null"
		# yes(1) replicates a line forever; head bounds it to N.
		yes "SELECT ${expr} FROM ${table};" | head -n 1000000
	} | psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X >/dev/null 2>&1 &
	PSQL_LOOP_PID=$!
}

kill_loop() {
	if [[ -n "${PSQL_LOOP_PID:-}" ]]; then
		kill -9 "${PSQL_LOOP_PID}" 2>/dev/null || true
		# Also kill any orphan psql still draining the stdin queue
		pkill -9 -P "${PSQL_LOOP_PID}" 2>/dev/null || true
		unset PSQL_LOOP_PID
	fi
}

profile_one() {
	local label="$1" table="$2" tag="$3"
	local data="${RESULTS_DIR}/perf-${RUN_TAG}-${label}-${tag}.data"
	local txt="${RESULTS_DIR}/perf-${RUN_TAG}-${label}-${tag}.report"
	local expr="${EXPR[${label}]:-}"
	if [[ -z "${expr}" ]]; then
		echo "unknown query label: ${label}" >&2
		exit 1
	fi
	local pid
	pid=$(pgrep -f "${SERENED_BIN}" | head -1)
	[[ -z "${pid}" ]] && {
		echo "serened pid not found" >&2
		exit 1
	}
	echo
	echo "=== profile ${label} (${tag}) ${DURATION}s -- pid ${pid} ==="
	# warmup
	psql "${PSQL_CONN}" -c "SET threads=1;" \
		-c "SET search_path TO public, native_db.main;" \
		-c "SELECT ${expr} FROM ${table};" >/dev/null
	# Start the workload first so the JIT/codec is warm by the time
	# perf starts sampling.  Wait briefly, then start perf, sleep, end.
	# No callgraph: dwarf produces 100+MB per profile which makes
	# `perf report` minute-long, and the binary has no frame pointers
	# so `--call-graph fp` would be useless.  Flat profile is enough
	# to identify hot symbols.
	run_loop "${table}" "${expr}"
	sleep 1
	perf record -F 999 --pid="${pid}" --inherit \
		-o "${data}" -- sleep "${DURATION}"
	kill_loop
	# Flat top-symbols report.
	perf report --no-children -i "${data}" --stdio --percent-limit 0.5 \
		--sort overhead,dso,symbol 2>/dev/null |
		sed -n '/^#\s*Overhead/,/^# (Tip\|^$/p' >"${txt}"
	echo "wrote ${txt}"
}

start_server
setup_session

for q in ${QUERIES}; do
	profile_one "${q}" "bench_idx" "cs"
	profile_one "${q}" "bench_native" "native"
done

killall -9 serened >/dev/null 2>&1 || true
echo
echo "=== reports ==="
ls -1 "${RESULTS_DIR}"/perf-"${RUN_TAG}"-*.report
