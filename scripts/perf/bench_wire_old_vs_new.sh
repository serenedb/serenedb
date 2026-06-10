#!/usr/bin/env bash
# bench_wire_old_vs_new.sh -- transport/protocol benchmark: the OLD pg-wire
# stack (pg_comm_task + general_server, --server_endpoints) vs the NEW one
# (server/network/, --network_pg_endpoint), from the SAME binary.
#
# Both servers embed the same DuckDB engine, so a cheap query (SELECT 1)
# isolates exactly the layer that differs: socket read -> frame decode ->
# protocol state machine -> result encode -> socket write. The point is not
# query speed; it is per-request transport cost.
#
# Each server is its own process with its own datadir, and is started, fully
# benchmarked, and KILLED before the other is ever launched -- the two never
# co-reside, so one server's (spinning) worker threads cannot steal cores from
# the one being measured. pgbench drives sustained QPS; perf stat -p <pid>
# captures cycles/instructions for the same window, so we report cycles-per-query
# (thread-count independent) alongside tps and latency.
#
# Pre-reqs: build_perf binary, pgbench, perf. No other serened on the ports.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
RESULTS_DIR="${ROOT}/scripts/perf/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${RESULTS_DIR}/wire-old-vs-new-${STAMP}"

PORT_OLD="${PERF_WIRE_PORT_OLD:-6480}"
PORT_NEW="${PERF_WIRE_PORT_NEW:-6481}"

CLIENTS="${PERF_WIRE_CLIENTS:-16}"
JOBS="${PERF_WIRE_JOBS:-8}"
DURATION="${PERF_WIRE_DURATION:-15}"
# Pin both servers to the same io-thread budget so the comparison is
# resources-equal, not architecture-default vs architecture-default.
IO_THREADS="${PERF_WIRE_IO_THREADS:-8}"
# Cap DuckDB's worker pool to the reserved CPU count. DuckDB sizes its pool from
# the HOST core count regardless of the cgroup cpuset, so without this it spawns
# ~32 workers oversubscribed onto the reserved cores. `nproc` honors the cpuset
# (this runs inside the partition), so it is exactly the reserved logical CPUs
# (16 for 8 physical cores, 24 for 12, ...). Set PERF_WIRE_CPU_THREADS to 0/""
# to leave DuckDB's default.
CPU_THREADS="${PERF_WIRE_CPU_THREADS-$(nproc)}"
# Post-measurement profiling: after the timed runs, while the server is still the
# only serened alive, perf-record call-graphs per workload so we can see WHERE the
# cycles go (stat counters only say how many). Set PERF_WIRE_PROFILE=0 to skip.
PROFILE="${PERF_WIRE_PROFILE:-1}"
PROFILE_SECS="${PERF_PROFILE_SECS:-8}"
PERF_FREQ="${PERF_FREQ:-999}"

# --- cached baselines -------------------------------------------------------
# The OLD server lives in the SAME binary as the new one, so its numbers only
# change when the binary's old-path / DuckDB compute changes: measure it once,
# cache it, reuse it across new-server iterations. Real PostgreSQL (Docker) is an
# external reference, also measured once and reused. Caches live outside the
# timestamped run dir so they persist across runs.
#   PERF_REMEASURE_OLD=1  refresh the old baseline (e.g. after a DuckDB change)
#   PERF_PG=1             include a real-PostgreSQL (Docker) column
#   PERF_REMEASURE_PG=1   refresh the postgres baseline
#   PERF_PG_ONLY=1        measure + cache ONLY postgres, then exit (used to seed the
#                         cache outside the cgroup partition; see run_bench_isolated.sh)
BASELINE_DIR="${PERF_BASELINE_DIR:-${RESULTS_DIR}/baselines}"
OLD_CACHE="${BASELINE_DIR}/old.tsv"
PG_CACHE="${BASELINE_DIR}/pg.tsv"
REMEASURE_OLD="${PERF_REMEASURE_OLD:-0}"
PG_ENABLE="${PERF_PG:-0}"
REMEASURE_PG="${PERF_REMEASURE_PG:-0}"
PG_ONLY="${PERF_PG_ONLY:-0}"
# Reuse the cached pg baseline only, never measure it here. run_bench_isolated.sh
# sets this for the in-partition run: a container inside the exclusive partition
# can't use the reserved cores, so pg must be measured outside (seeded) and reused.
PG_REUSE_ONLY="${PERF_PG_REUSE_ONLY:-0}"
PG_IMAGE="${PERF_PG_IMAGE:-postgres:latest}"
PG_PORT="${PERF_PG_PORT:-6482}"
PG_CONTAINER="${PERF_PG_CONTAINER:-sdb_wire_pg}"
# Cores to pin the postgres container to, so it races on the same CPUs as serened.
# run_bench_isolated.sh exports the reserved set here; empty = docker uses all cores.
PG_CPUSET="${PERF_BENCH_CORES:-}"

if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN} -- build the perf binary first" >&2
	exit 1
fi
for tool in pgbench perf; do
	command -v "${tool}" >/dev/null 2>&1 || {
		echo "${tool} not found" >&2
		exit 1
	}
done

mkdir -p "${OUT_DIR}"
DATA_OLD="$(mktemp -d /tmp/sdb_wire_old.XXXXXX)"
DATA_NEW="$(mktemp -d /tmp/sdb_wire_new.XXXXXX)"
CUR_PID=""

cleanup() {
	[[ -n "${CUR_PID}" ]] && kill -9 "${CUR_PID}" 2>/dev/null || true
	rm -rf "${DATA_OLD}" "${DATA_NEW}"
}
trap cleanup EXIT

wait_up() {
	local port="$1" log="$2"
	for _ in $(seq 1 60); do
		if psql "postgres://postgres@127.0.0.1:${port}/postgres" \
			-c 'SELECT 1' >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.5
	done
	echo "server on :${port} did not come up:" >&2
	tail -40 "${log}" >&2
	return 1
}

echo "binary:   ${SERENED_BIN}"
echo "out:      ${OUT_DIR}"
echo "clients:  ${CLIENTS}  jobs: ${JOBS}  duration: ${DURATION}s  io_threads: ${IO_THREADS}"
echo

# Workload scripts. Keyed name -> SQL; protocol chosen per run below.
declare -A WL=()
WL[select1]="SELECT 1;"
WL[rows]="SELECT i, i::text AS s, i::float8 AS f FROM generate_series(1,1000) g(i);"
# Analytical: a parallel hash-aggregate over 3M rows returning 997 groups. Unlike
# the transport workloads, this fans out across the DuckDB worker pool, so the
# query pump rides the reschedule path (NO_TASKS_AVAILABLE while workers run the
# aggregate) -- the case the new stack's per-hop scheduler churn is measured on.
# Small result (997 rows) keeps execution+reschedule, not encoding, the cost.
WL[analytical]="SELECT k, count(*) c, sum(i) s FROM (SELECT i, i % 997 AS k FROM generate_series(1,3000000) g(i)) t GROUP BY k ORDER BY k;"
# Parse-cost probe: predicate-heavy so parsing is non-trivial, but constant-folds to
# a 1-row trivial execution. So prepared (parse cached) is as cheap as SELECT 1, and
# the simple/extended-vs-prepared gap is PURELY per-query parse cost. PG parses it
# cheaply (flat across protocols), so new/pg on the simple/extended rows isolates our
# parser overhead -- unlike `rows`, whose modes are dominated by execution+encoding.
WL[wide]="SELECT 1 WHERE 1=1 AND 2=2 AND 3=3 AND 4=4 AND 5=5 AND 6=6 AND 7=7 AND 8=8 AND 9=9 AND 10=10;"
for name in "${!WL[@]}"; do
	printf '%s\n' "${WL[$name]}" >"${OUT_DIR}/wl_${name}.sql"
done

# run_one <label> <port> <pid> <sqlfile> <pgbench-mode> [extra pgbench args]
# Returns nothing; appends a parsed row to ${OUT_DIR}/results.tsv.
run_one() {
	local label="$1" port="$2" pid="$3" sqlfile="$4" mode="$5"
	shift 5
	local extra=("$@")
	local conn=(-h 127.0.0.1 -p "${port}" -U postgres postgres)
	local pgb_log="${OUT_DIR}/pgbench_${label}.log"
	local stat_log="${OUT_DIR}/stat_${label}.txt"

	# Warm the connections/JIT before the measured window.
	pgbench "${conn[@]}" -n -f "${sqlfile}" -M "${mode}" \
		-c "${CLIENTS}" -j "${JOBS}" -T 2 "${extra[@]}" \
		>/dev/null 2>&1 || true

	pgbench "${conn[@]}" -n -f "${sqlfile}" -M "${mode}" \
		-c "${CLIENTS}" -j "${JOBS}" -T "${DURATION}" -r "${extra[@]}" \
		>"${pgb_log}" 2>&1 &
	local pgb_pid=$!
	# Sample the server for slightly less than the pgbench window so perf stops
	# while load is still flowing (clean steady-state counters). External engines
	# (postgres in docker) have no local pid to attach to -> tps/latency only.
	if [[ -n "${pid}" && "${pid}" != "-" ]]; then
		perf stat -p "${pid}" -e task-clock,cycles,instructions \
			-o "${stat_log}" -- sleep "$((DURATION - 2))" >/dev/null 2>&1 || true
	else
		sleep "$((DURATION - 2))"
	fi
	wait "${pgb_pid}" 2>/dev/null || true

	local tps lat cycles insns txns
	tps=$(awk -F'= ' '/tps = .*without initial/{print $2+0}' "${pgb_log}")
	[[ -z "${tps}" ]] && tps=$(awk -F'= ' '/^tps =/{print $2+0; exit}' "${pgb_log}")
	lat=$(awk -F'= ' '/latency average/{print $2+0}' "${pgb_log}")
	txns=$(awk '/number of transactions actually processed/{print $NF+0}' "${pgb_log}")
	if [[ -f "${stat_log}" ]]; then
		cycles=$(awk '/cycles/{gsub(/[^0-9]/,"",$1); print $1; exit}' "${stat_log}")
		insns=$(awk '/instructions/{gsub(/[^0-9]/,"",$1); print $1; exit}' "${stat_log}")
	fi

	printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
		"${label}" "${tps}" "${lat}" "${txns}" "${cycles:-0}" "${insns:-0}" \
		>>"${OUT_DIR}/results.tsv"
	printf '  %-22s tps=%-12s lat=%-9s txns=%s\n' "${label}" "${tps}" "${lat}" "${txns}"
}

# profile_one <label> <port> <pid> <sqlfile> <pgbench-mode>
# Separate (post-measurement) pass: perf-record the server's call graph under the
# same workload, then dump self-time (top_symbols) and callee-graph (top_stacks).
# Records on fp call-graphs -- build_perf keeps frame pointers + debug info.
profile_one() {
	local label="$1" port="$2" pid="$3" sqlfile="$4" mode="$5"
	local conn=(-h 127.0.0.1 -p "${port}" -U postgres postgres)
	local data="${OUT_DIR}/perf_${label}.data"

	# Drive load for slightly longer than the record window so samples land in
	# steady state, not in pgbench ramp-up/tear-down.
	pgbench "${conn[@]}" -n -f "${sqlfile}" -M "${mode}" \
		-c "${CLIENTS}" -j "${JOBS}" -T "$((PROFILE_SECS + 2))" \
		>"${OUT_DIR}/pgbench_prof_${label}.log" 2>&1 &
	local pgb=$!
	perf record -F "${PERF_FREQ}" -g --call-graph fp --output "${data}" \
		--pid "${pid}" -- sleep "${PROFILE_SECS}" >/dev/null 2>&1 || true
	wait "${pgb}" 2>/dev/null || true

	perf report --no-children --stdio -g none --input "${data}" 2>/dev/null |
		head -45 >"${OUT_DIR}/top_symbols_${label}.txt" || true
	perf report --stdio -g graph,1.0,callee --input "${data}" 2>/dev/null |
		head -160 >"${OUT_DIR}/top_stacks_${label}.txt" || true
	printf '  profiled %-22s -> top_symbols_%s.txt\n' "${label}" "${label}"
}

: >"${OUT_DIR}/results.tsv"

# Benchmark ONE server fully, then kill it before the other is ever started, so
# the two never co-reside and an idle server's worker threads cannot pollute the
# measured one. This is the whole point of using separate processes.
bench_server() {
	local srv="$1" port="$2" datadir="$3"
	shift 3
	local start_args=("$@")
	local cpu_args=()
	[[ -n "${CPU_THREADS}" ]] && cpu_args=(--server_cpu_threads "${CPU_THREADS}")
	echo "-- ${srv} server (:${port}) [isolated; no other serened alive]${CPU_THREADS:+; cpu_threads=${CPU_THREADS}}"
	"${SERENED_BIN}" "${datadir}" \
		--server_io_threads "${IO_THREADS}" "${cpu_args[@]}" "${start_args[@]}" \
		>"${OUT_DIR}/serened_${srv}.log" 2>&1 &
	CUR_PID=$!
	wait_up "${port}" "${OUT_DIR}/serened_${srv}.log"
	run_one "${srv}_select1_simple" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" simple
	run_one "${srv}_select1_extended" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" extended
	run_one "${srv}_select1_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" prepared
	run_one "${srv}_wide_simple" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_wide.sql" simple
	run_one "${srv}_wide_extended" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_wide.sql" extended
	run_one "${srv}_wide_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_wide.sql" prepared
	run_one "${srv}_rows_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_rows.sql" prepared
	run_one "${srv}_analytical_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_analytical.sql" prepared
	if [[ "${PROFILE}" != "0" ]]; then
		echo "  -- profiling ${srv} (perf record ${PROFILE_SECS}s/workload, still isolated)"
		profile_one "${srv}_select1_simple" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" simple
		profile_one "${srv}_select1_extended" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" extended
		profile_one "${srv}_select1_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" prepared
		profile_one "${srv}_wide_simple" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_wide.sql" simple
		profile_one "${srv}_wide_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_wide.sql" prepared
		profile_one "${srv}_rows_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_rows.sql" prepared
		profile_one "${srv}_analytical_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_analytical.sql" prepared
	fi
	kill -9 "${CUR_PID}" 2>/dev/null || true
	wait "${CUR_PID}" 2>/dev/null || true
	CUR_PID=""
	sleep 1
}

# Measure a real PostgreSQL (docker) -- an external reference. No local pid to
# perf-stat, so tps/latency only; pinned to PG_CPUSET so it races on the same
# cores. Appends pg_<workload> rows to results.tsv. Must NOT run inside the
# exclusive cgroup partition (the kernel denies its container cgroup the reserved
# cores) -- run_bench_isolated.sh seeds this cache before reserving.
bench_postgres() {
	if ! command -v docker >/dev/null 2>&1; then
		echo "  postgres: docker not found -- skipping" >&2
		return 1
	fi
	local cpuset=()
	[[ -n "${PG_CPUSET}" ]] && cpuset=(--cpuset-cpus "${PG_CPUSET}")
	echo "-- postgres ${PG_IMAGE} (:${PG_PORT}) [docker${PG_CPUSET:+, cpuset ${PG_CPUSET}}]"
	docker rm -f "${PG_CONTAINER}" >/dev/null 2>&1 || true
	if ! docker run -d --name "${PG_CONTAINER}" "${cpuset[@]}" \
		-e POSTGRES_HOST_AUTH_METHOD=trust -p "${PG_PORT}:5432" \
		"${PG_IMAGE}" -c max_connections=300 -c shared_buffers=512MB >/dev/null; then
		echo "  postgres: docker run failed -- skipping" >&2
		return 1
	fi
	local up=0
	for _ in $(seq 1 90); do
		if psql "postgres://postgres@127.0.0.1:${PG_PORT}/postgres" -c 'SELECT 1' >/dev/null 2>&1; then
			up=1
			break
		fi
		sleep 1
	done
	if [[ "${up}" != 1 ]]; then
		echo "  postgres did not come up:" >&2
		docker logs "${PG_CONTAINER}" 2>&1 | tail -20 >&2
		docker rm -f "${PG_CONTAINER}" >/dev/null 2>&1 || true
		return 1
	fi
	run_one pg_select1_simple "${PG_PORT}" "-" "${OUT_DIR}/wl_select1.sql" simple
	run_one pg_select1_extended "${PG_PORT}" "-" "${OUT_DIR}/wl_select1.sql" extended
	run_one pg_select1_prepared "${PG_PORT}" "-" "${OUT_DIR}/wl_select1.sql" prepared
	run_one pg_wide_simple "${PG_PORT}" "-" "${OUT_DIR}/wl_wide.sql" simple
	run_one pg_wide_extended "${PG_PORT}" "-" "${OUT_DIR}/wl_wide.sql" extended
	run_one pg_wide_prepared "${PG_PORT}" "-" "${OUT_DIR}/wl_wide.sql" prepared
	run_one pg_rows_prepared "${PG_PORT}" "-" "${OUT_DIR}/wl_rows.sql" prepared
	run_one pg_analytical_prepared "${PG_PORT}" "-" "${OUT_DIR}/wl_analytical.sql" prepared
	docker rm -f "${PG_CONTAINER}" >/dev/null 2>&1 || true
}

mkdir -p "${BASELINE_DIR}"

# PG-only seeding mode: measure + cache postgres, then exit (run outside the
# cgroup partition so the container can use the reserved cores).
if [[ "${PG_ONLY}" == 1 ]]; then
	echo "=== postgres-only: measuring + caching baseline ==="
	if bench_postgres; then
		grep '^pg_' "${OUT_DIR}/results.tsv" >"${PG_CACHE}"
		echo "cached postgres baseline -> ${PG_CACHE}"
		exit 0
	fi
	echo "postgres baseline NOT cached (measurement failed)" >&2
	exit 1
fi

echo
echo "=== benchmarking (old/pg cached + reused; new always measured) ==="
# OLD -- same binary as new, so cache and reuse across new-server iterations.
if [[ -s "${OLD_CACHE}" && "${REMEASURE_OLD}" != 1 ]]; then
	echo "-- old: reusing cached baseline (${OLD_CACHE}); PERF_REMEASURE_OLD=1 to refresh"
	cat "${OLD_CACHE}" >>"${OUT_DIR}/results.tsv"
else
	bench_server old "${PORT_OLD}" "${DATA_OLD}" --server_endpoints "pgsql+tcp://0.0.0.0:${PORT_OLD}"
	grep '^old_' "${OUT_DIR}/results.tsv" >"${OLD_CACHE}" && echo "  cached old baseline -> ${OLD_CACHE}"
fi
# POSTGRES -- optional external reference, cached. reuse-only forces using the
# cache (it was seeded outside the cgroup partition), ignoring REMEASURE_PG.
if [[ "${PG_ENABLE}" == 1 ]]; then
	if [[ -s "${PG_CACHE}" && ("${REMEASURE_PG}" != 1 || "${PG_REUSE_ONLY}" == 1) ]]; then
		echo "-- postgres: reusing cached baseline (${PG_CACHE}); PERF_REMEASURE_PG=1 to refresh"
		cat "${PG_CACHE}" >>"${OUT_DIR}/results.tsv"
	elif [[ "${PG_REUSE_ONLY}" == 1 ]]; then
		echo "-- postgres: reuse-only and no cache present -- skipping pg column" >&2
	elif bench_postgres; then
		grep '^pg_' "${OUT_DIR}/results.tsv" >"${PG_CACHE}" && echo "  cached postgres baseline -> ${PG_CACHE}"
	fi
fi
# NEW -- always measured.
bench_server new "${PORT_NEW}" "${DATA_NEW}" --network_pg_endpoint "pgsql://0.0.0.0:${PORT_NEW}"

# Summary table: pair old vs new per workload, with cycles-per-query.
echo
echo "================== WIRE BENCH SUMMARY =================="
echo "clients=${CLIENTS} jobs=${JOBS} dur=${DURATION}s io_threads=${IO_THREADS}"
echo
awk -F'\t' '
{
  key=$1; sub(/^old_/,"",key); sub(/^new_/,"",key); sub(/^pg_/,"",key);
  side = ($1 ~ /^old_/) ? "old" : ($1 ~ /^pg_/) ? "pg" : "new";
  if (side=="pg") have_pg=1;
  tps[key,side]=$2; lat[key,side]=$3; txn[key,side]=$4; cyc[key,side]=$5;
  if (!(key in seen)) { order[++n]=key; seen[key]=1 }
}
END {
  if (have_pg) {
    printf "%-20s %11s %11s %11s %8s %8s %12s %12s\n","workload","tps(old)","tps(pg)","tps(new)","new/old","new/pg","cyc/q(old)","cyc/q(new)";
    printf "%-20s %11s %11s %11s %8s %8s %12s %12s\n","--------------------","-----------","-----------","-----------","--------","--------","------------","------------";
  } else {
    printf "%-20s %12s %12s %10s %14s %14s\n","workload","tps(old)","tps(new)","new/old","cyc/q(old)","cyc/q(new)";
    printf "%-20s %12s %12s %10s %14s %14s\n","--------------------","------------","------------","----------","--------------","--------------";
  }
  for (i=1;i<=n;i++) {
    k=order[i];
    to=tps[k,"old"]+0; tn=tps[k,"new"]+0; tp=tps[k,"pg"]+0;
    rno=(to>0)?sprintf("%.2fx",tn/to):"n/a";
    cqo=(txn[k,"old"]+0>0)?sprintf("%.0f",cyc[k,"old"]/txn[k,"old"]):"n/a";
    cqn=(txn[k,"new"]+0>0)?sprintf("%.0f",cyc[k,"new"]/txn[k,"new"]):"n/a";
    if (have_pg) {
      rnp=(tp>0)?sprintf("%.2fx",tn/tp):"n/a";
      printf "%-20s %11.1f %11.1f %11.1f %8s %8s %12s %12s\n",k,to,tp,tn,rno,rnp,cqo,cqn;
    } else {
      printf "%-20s %12.1f %12.1f %10s %14s %14s\n",k,to,tn,rno,cqo,cqn;
    }
  }
}' "${OUT_DIR}/results.tsv" | tee "${OUT_DIR}/summary.txt"
echo "======================================================="
echo
echo "results: ${OUT_DIR}"
if [[ "${PROFILE}" != "0" ]]; then
	echo "profiles: top_symbols_<srv>_<workload>.txt (self time), top_stacks_* (callee graph),"
	echo "          perf_*.data (open with: perf report -i <file>, or fold for a flamegraph)"
	echo "          compare e.g.: diff <(cat ${OUT_DIR}/top_symbols_old_rows_prepared.txt) \\"
	echo "                            <(cat ${OUT_DIR}/top_symbols_new_rows_prepared.txt)"
fi
