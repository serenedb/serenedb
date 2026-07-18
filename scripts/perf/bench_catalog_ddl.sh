#!/usr/bin/env bash
# bench_catalog_ddl.sh -- catalog-plane A/B: baseline binary (pre-split duckdb
# catalog tables) vs branch perf binary (catalog wal + injected indexes).
#
# Three headline rates, each interleaved A/B/A/B across REPS rounds on fresh
# datadirs (sequential sweeps drift):
#   seq      -- nextval throughput, one query of SEQ_CALLS nextval() calls.
#               Baseline persists every call; the branch logs a 32-call
#               horizon per append.
#   seq_stmt -- nextval as STMT_CALLS separate statements on one connection
#               (per-statement fsync pressure, the pg-wire shape).
#   ddl      -- CREATE TABLE + DROP TABLE pairs per second, DDL_PAIRS pairs.
#
# Pre-reqs: psql, both binaries, nothing on PORT.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
NEW_BIN="${PERF_CAT_NEW_BIN:-${BUILD_DIR}/bin/serened}"
OLD_BIN="${PERF_CAT_OLD_BIN:-${HOME}/baselines/serened-split-baseline-9411d5732}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${RESULTS_DIR}/catalog-ddl-${STAMP}.log"

PORT="${PERF_CAT_PORT:-6497}"
REPS="${PERF_CAT_REPS:-3}"
SEQ_CALLS="${PERF_CAT_SEQ_CALLS:-20000}"
STMT_CALLS="${PERF_CAT_STMT_CALLS:-2000}"
DDL_PAIRS="${PERF_CAT_DDL_PAIRS:-200}"

for bin in "${OLD_BIN}" "${NEW_BIN}"; do
	[[ -x "${bin}" ]] || {
		echo "missing binary: ${bin}" >&2
		exit 1
	}
done

mkdir -p "${RESULTS_DIR}"
CONN="postgres://postgres@localhost:${PORT}/postgres"
SERENED_PID=""
DATA_DIR=""

stop_server() {
	[[ -n "${SERENED_PID}" ]] && kill -9 "${SERENED_PID}" >/dev/null 2>&1 || true
	SERENED_PID=""
	[[ -n "${DATA_DIR}" ]] && rm -rf "${DATA_DIR}"
	DATA_DIR=""
}
trap stop_server EXIT

start_server() { # $1 = binary
	DATA_DIR="$(mktemp -d "/tmp/${USER}-catddl-XXXXXX")"
	"$1" "${DATA_DIR}" --listen "postgres://0.0.0.0:${PORT}" \
		>"${DATA_DIR}/serened.log" 2>&1 &
	SERENED_PID=$!
	for _ in $(seq 1 60); do
		psql "${CONN}" -c 'SELECT 1' >/dev/null 2>&1 && return 0
		sleep 0.5
	done
	echo "server failed to start" >&2
	exit 1
}

now_ms() { date +%s%3N; }

bench_seq() {
	psql "${CONN}" -q -c "CREATE SEQUENCE bs" >/dev/null
	local t0 t1
	t0=$(now_ms)
	psql "${CONN}" -qtA \
		-c "SELECT count(nextval('bs')) FROM range(${SEQ_CALLS})" >/dev/null
	t1=$(now_ms)
	echo "$((SEQ_CALLS * 1000 / (t1 - t0)))"
}

bench_seq_stmt() {
	psql "${CONN}" -q -c "CREATE SEQUENCE bss" >/dev/null
	local f t0 t1
	f="$(mktemp)"
	for _ in $(seq 1 "${STMT_CALLS}"); do
		echo "SELECT nextval('bss');" >>"${f}"
	done
	t0=$(now_ms)
	psql "${CONN}" -q -o /dev/null -f "${f}"
	t1=$(now_ms)
	rm -f "${f}"
	echo "$((STMT_CALLS * 1000 / (t1 - t0)))"
}

bench_ddl() {
	local f t0 t1
	f="$(mktemp)"
	for i in $(seq 1 "${DDL_PAIRS}"); do
		echo "CREATE TABLE ddl_t${i} (a INT PRIMARY KEY, b TEXT, c BIGINT);" >>"${f}"
		echo "DROP TABLE ddl_t${i};" >>"${f}"
	done
	t0=$(now_ms)
	psql "${CONN}" -q -o /dev/null -f "${f}"
	t1=$(now_ms)
	rm -f "${f}"
	echo "$((DDL_PAIRS * 2 * 1000 / (t1 - t0)))"
}

run_round() { # $1 = label, $2 = binary
	start_server "$2"
	local seq stmt ddl
	seq=$(bench_seq)
	stmt=$(bench_seq_stmt)
	ddl=$(bench_ddl)
	stop_server
	echo "$1 seq=${seq}/s seq_stmt=${stmt}/s ddl=${ddl}/s" | tee -a "${OUT}"
}

echo "old=${OLD_BIN}" | tee -a "${OUT}"
echo "new=${NEW_BIN}" | tee -a "${OUT}"
for rep in $(seq 1 "${REPS}"); do
	run_round "old[${rep}]" "${OLD_BIN}"
	run_round "new[${rep}]" "${NEW_BIN}"
done
echo "results in ${OUT}"
