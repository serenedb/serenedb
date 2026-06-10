#!/usr/bin/env bash
# profile_wire.sh -- call-graph profiles for the OLD vs NEW pg-wire stacks
# under the same transport workloads bench_wire_old_vs_new.sh measures.
# Companion to that script: the bench tells us NEW burns more cycles/query;
# this tells us *where*. perf record --pid samples only the server process,
# top_symbols.txt = self time, top_stacks.txt = callee graph.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
RESULTS_DIR="${ROOT}/scripts/perf/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${RESULTS_DIR}/wire-profile-${STAMP}"

PORT_OLD="${PERF_WIRE_PORT_OLD:-6480}"
PORT_NEW="${PERF_WIRE_PORT_NEW:-6481}"
CLIENTS="${PERF_WIRE_CLIENTS:-16}"
JOBS="${PERF_WIRE_JOBS:-8}"
DURATION="${PERF_WIRE_DURATION:-12}"
IO_THREADS="${PERF_WIRE_IO_THREADS:-8}"
FREQ="${PERF_FREQ:-999}"

if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN}" >&2
	exit 1
fi
for tool in pgbench perf; do
	command -v "${tool}" >/dev/null 2>&1 || { echo "${tool} not found" >&2; exit 1; }
done

mkdir -p "${OUT_DIR}"
DATA_OLD="$(mktemp -d /tmp/sdb_prof_old.XXXXXX)"
DATA_NEW="$(mktemp -d /tmp/sdb_prof_new.XXXXXX)"
PID_OLD=""
PID_NEW=""
cleanup() {
	[[ -n "${PID_OLD}" ]] && kill -9 "${PID_OLD}" 2>/dev/null || true
	[[ -n "${PID_NEW}" ]] && kill -9 "${PID_NEW}" 2>/dev/null || true
	rm -rf "${DATA_OLD}" "${DATA_NEW}"
}
trap cleanup EXIT

wait_up() {
	local port="$1" log="$2"
	for _ in $(seq 1 60); do
		psql "postgres://postgres@127.0.0.1:${port}/postgres" -c 'SELECT 1' \
			>/dev/null 2>&1 && return 0
		sleep 0.5
	done
	echo "server on :${port} did not come up:" >&2
	tail -40 "${log}" >&2
	return 1
}

echo "out: ${OUT_DIR}"
"${SERENED_BIN}" "${DATA_OLD}" --server_io_threads "${IO_THREADS}" \
	--server_endpoints "pgsql+tcp://0.0.0.0:${PORT_OLD}" \
	>"${OUT_DIR}/serened_old.log" 2>&1 &
PID_OLD=$!
wait_up "${PORT_OLD}" "${OUT_DIR}/serened_old.log"
"${SERENED_BIN}" "${DATA_NEW}" --server_io_threads "${IO_THREADS}" \
	--network_pg_endpoint "pgsql://0.0.0.0:${PORT_NEW}" \
	>"${OUT_DIR}/serened_new.log" 2>&1 &
PID_NEW=$!
wait_up "${PORT_NEW}" "${OUT_DIR}/serened_new.log"

printf 'SELECT 1;\n' >"${OUT_DIR}/wl_select1.sql"
printf 'SELECT i, i::text AS s, i::float8 AS f FROM generate_series(1,1000) g(i);\n' \
	>"${OUT_DIR}/wl_rows.sql"

# profile <srv> <port> <pid> <sqlfile> <mode>
profile() {
	local srv="$1" port="$2" pid="$3" sqlfile="$4" mode="$5"
	local tag="${srv}_$(basename "${sqlfile}" .sql)_${mode}"
	local conn=(-h 127.0.0.1 -p "${port}" -U postgres postgres)
	local data="${OUT_DIR}/perf_${tag}.data"
	echo "-- profiling ${tag}"
	pgbench "${conn[@]}" -n -f "${sqlfile}" -M "${mode}" \
		-c "${CLIENTS}" -j "${JOBS}" -T 2 >/dev/null 2>&1 || true
	pgbench "${conn[@]}" -n -f "${sqlfile}" -M "${mode}" \
		-c "${CLIENTS}" -j "${JOBS}" -T "${DURATION}" \
		>"${OUT_DIR}/pgbench_${tag}.log" 2>&1 &
	local pgb=$!
	perf record -F "${FREQ}" -g --call-graph fp --output "${data}" \
		--pid "${pid}" -- sleep "$((DURATION - 2))" >/dev/null 2>&1 || true
	wait "${pgb}" 2>/dev/null || true
	perf report --no-children --stdio -g none --input "${data}" 2>/dev/null |
		head -45 >"${OUT_DIR}/top_symbols_${tag}.txt" || true
	perf report --stdio -g graph,1.0,callee --input "${data}" 2>/dev/null |
		head -160 >"${OUT_DIR}/top_stacks_${tag}.txt" || true
}

for spec in "select1:simple" "select1:prepared" "rows:prepared"; do
	wl="${spec%%:*}"; mode="${spec##*:}"
	profile new "${PORT_NEW}" "${PID_NEW}" "${OUT_DIR}/wl_${wl}.sql" "${mode}"
	profile old "${PORT_OLD}" "${PID_OLD}" "${OUT_DIR}/wl_${wl}.sql" "${mode}"
done

echo
echo "profiles in ${OUT_DIR}"
ls "${OUT_DIR}"/top_symbols_*.txt