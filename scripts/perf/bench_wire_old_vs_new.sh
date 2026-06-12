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
# By default serened itself runs in docker (PERF_SDB_DOCKER=1) so it pays the same
# container networking/cgroup overhead as the external engines it is compared to;
# PERF_SDB_DOCKER=0 restores the bare-host-process behavior.
#
# Pre-reqs: build_perf binary, pgbench, perf, docker (unless PERF_SDB_DOCKER=0).
# No other serened on the ports.

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
# ~32 workers oversubscribed onto the reserved cores. When run_bench_isolated.sh
# exports PERF_BENCH_CORES, its cpu count is authoritative: `nproc` only matches it
# for the bare-process run inside the partition, not in docker mode where this
# shell is unpinned and only the container gets the cpuset. Set
# PERF_WIRE_CPU_THREADS to 0/"" to leave DuckDB's default.
cpuset_count() {
	local n=0 part
	local IFS=','
	for part in $1; do
		if [[ "${part}" == *-* ]]; then
			n=$((n + ${part#*-} - ${part%-*} + 1))
		else
			n=$((n + 1))
		fi
	done
	echo "${n}"
}
if [[ -n "${PERF_WIRE_CPU_THREADS+x}" ]]; then
	CPU_THREADS="${PERF_WIRE_CPU_THREADS}"
elif [[ -n "${PERF_BENCH_CORES:-}" ]]; then
	CPU_THREADS="$(cpuset_count "${PERF_BENCH_CORES}")"
else
	CPU_THREADS="$(nproc)"
fi
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
REMEASURE_OLD="${PERF_REMEASURE_OLD:-0}"
# Cores to pin the external (docker) engines to, so they race on the same CPUs as
# serened. run_bench_isolated.sh exports the reserved set here; empty = all cores.
ENG_CPUSET="${PERF_BENCH_CORES:-}"
# Serened-in-docker (the default): the external engines all run as containers, so a
# bare-host serened would skip the container bridge-networking + cgroup cost they
# all pay. Run serened the same way -- same --cpuset-cpus pinning, same bridge +
# published-port networking -- reusing the already-built binary (statically linked,
# host is ubuntu 24.04 like the image) via bind mount; nothing is compiled into an
# image. PERF_SDB_DOCKER=0 = bare host process (required inside the exclusive
# cgroup partition, where containers are denied the cores; run_bench_isolated.sh
# handles that split).
SDB_DOCKER="${PERF_SDB_DOCKER:-1}"
SDB_IMAGE="${PERF_SDB_IMAGE:-ubuntu:24.04}"
SDB_CONTAINER="${PERF_SDB_CONTAINER:-sdb_wire_serened}"

# --- external pg-wire engine registry --------------------------------------
# Real PostgreSQL and the other servers (CockroachDB / CedarDB / RisingWave /
# ClickHouse) all speak the Postgres wire protocol and all run in docker, so they
# share ONE mechanism:
# each is a row in this registry, cached the same way (own <name>.tsv baseline,
# measured/seeded OUTSIDE the cgroup partition since a container can't schedule on
# the reserved cores, reused inside it), and adds one summary column.
#
# Selection is by NAME, not a flag-per-engine, so you don't have to spell out every
# server. All comma-lists also accept "all" (every registered engine):
#   PERF_DB=pg,crdb        which engines to include   (default: none -> plain old/new)
#   PERF_DB=all            include every engine
#   PERF_REMEASURE_DB=all  refresh the listed engines' baselines (else reuse cache)
#   PERF_REUSE_ONLY_DB=all reuse cache only, never measure (set inside the partition)
#   PERF_SEED_ONLY=<name>  measure+cache ONE engine then exit (seed outside partition)
# Per-engine image/port/etc. stay overridable via PERF_<NAME>_IMAGE / _PORT / ...
# Back-compat: the old PERF_PG / PERF_REMEASURE_PG / PERF_PG_REUSE_ONLY / PERF_PG_ONLY
# / PERF_EXT_ONLY flags still work (folded into the lists below).
EXT_ORDER=(pg crdb cedar risingwave clickhouse) # display + iteration order; pg stays leftmost
declare -A ENG_IMAGE ENG_HPORT ENG_CPORT ENG_CONTAINER ENG_USER ENG_DB ENG_PASS
declare -A ENG_PREARGS ENG_CARGS # docker args before the image / container start args after it

# Real PostgreSQL: trust auth, pg wire on 5432.
ENG_IMAGE[pg]="${PERF_PG_IMAGE:-postgres:latest}"
ENG_HPORT[pg]="${PERF_PG_PORT:-6482}"
ENG_CPORT[pg]=5432
ENG_CONTAINER[pg]="${PERF_PG_CONTAINER:-sdb_wire_pg}"
ENG_USER[pg]=postgres
ENG_DB[pg]=postgres
ENG_PASS[pg]=""
ENG_PREARGS[pg]="-e POSTGRES_HOST_AUTH_METHOD=trust"
ENG_CARGS[pg]="-c max_connections=300 -c shared_buffers=512MB"

# CockroachDB: insecure single-node, SQL on 26257, user root / db defaultdb / no pw.
ENG_IMAGE[crdb]="${PERF_CRDB_IMAGE:-cockroachdb/cockroach:latest}"
ENG_HPORT[crdb]="${PERF_CRDB_PORT:-6483}"
ENG_CPORT[crdb]=26257
ENG_CONTAINER[crdb]="${PERF_CRDB_CONTAINER:-sdb_wire_crdb}"
ENG_USER[crdb]=root
ENG_DB[crdb]=defaultdb
ENG_PASS[crdb]=""
ENG_PREARGS[crdb]=""
ENG_CARGS[crdb]="start-single-node --insecure"

# CedarDB: pg wire on 5432, user postgres / db postgres, but no trust-auth mode --
# a password is mandatory, passed to psql/pgbench via PGPASSWORD.
ENG_IMAGE[cedar]="${PERF_CEDAR_IMAGE:-cedardb/cedardb}"
ENG_HPORT[cedar]="${PERF_CEDAR_PORT:-6484}"
ENG_CPORT[cedar]=5432
ENG_CONTAINER[cedar]="${PERF_CEDAR_CONTAINER:-sdb_wire_cedar}"
ENG_USER[cedar]=postgres
ENG_DB[cedar]=postgres
ENG_PASS[cedar]="${PERF_CEDAR_PASSWORD:-bench}"
ENG_PREARGS[cedar]="-e CEDAR_PASSWORD=${PERF_CEDAR_PASSWORD:-bench}"
ENG_CARGS[cedar]=""

# RisingWave: a Rust streaming DB, natively pg-wire. single_node mode listens on
# 4566, user root / db dev / no password. (Replaces GlareDB, whose current 25.x line
# dropped the persistent pg-wire server and is now embeddable/CLI/WASM only.)
ENG_IMAGE[risingwave]="${PERF_RISINGWAVE_IMAGE:-risingwavelabs/risingwave:latest}"
ENG_HPORT[risingwave]="${PERF_RISINGWAVE_PORT:-6485}"
ENG_CPORT[risingwave]=4566
ENG_CONTAINER[risingwave]="${PERF_RISINGWAVE_CONTAINER:-sdb_wire_risingwave}"
ENG_USER[risingwave]=root
ENG_DB[risingwave]=dev
ENG_PASS[risingwave]=""
ENG_PREARGS[risingwave]=""
ENG_CARGS[risingwave]="single_node"

# ClickHouse: enable its pg-wire listener (postgresql_port) via a server config
# override arg; the pg interface requires a password, so set one and create the
# user/db through the image's env vars. NOTE: ClickHouse's pg-wire + SQL dialect is
# only partially Postgres-compatible -- the rows/analytical/wide workloads (and the
# extended/prepared modes) may not all run; run_one tolerates a failed pgbench and
# records 0 tps for those rows rather than aborting the bench.
ENG_IMAGE[clickhouse]="${PERF_CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server:latest}"
ENG_HPORT[clickhouse]="${PERF_CLICKHOUSE_PORT:-6486}"
ENG_CPORT[clickhouse]=9005
ENG_CONTAINER[clickhouse]="${PERF_CLICKHOUSE_CONTAINER:-sdb_wire_clickhouse}"
ENG_USER[clickhouse]="${PERF_CLICKHOUSE_USER:-default}"
ENG_DB[clickhouse]="${PERF_CLICKHOUSE_DB:-default}"
ENG_PASS[clickhouse]="${PERF_CLICKHOUSE_PASSWORD:-bench}"
ENG_PREARGS[clickhouse]="-e CLICKHOUSE_PASSWORD=${PERF_CLICKHOUSE_PASSWORD:-bench} -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 --ulimit nofile=262144:262144"
ENG_CARGS[clickhouse]="--postgresql_port=9005"

# Expand a comma list (or "all") of engine names into a deduped, registry-ordered,
# validated set. Used for PERF_DB / PERF_REMEASURE_DB / PERF_REUSE_ONLY_DB.
expand_engine_list() {
	local raw="$1" name
	declare -A want=()
	local IFS=','
	for name in $raw; do
		[[ -z "${name}" ]] && continue
		if [[ "${name}" == all ]]; then
			for name in "${EXT_ORDER[@]}"; do want[$name]=1; done
			continue
		fi
		if [[ -z "${ENG_IMAGE[$name]+x}" ]]; then
			echo "unknown engine '${name}' (known: ${EXT_ORDER[*]}, all)" >&2
			return 1
		fi
		want[$name]=1
	done
	local out=()
	for name in "${EXT_ORDER[@]}"; do [[ -n "${want[$name]+x}" ]] && out+=("$name"); done
	# Emit space-separated (IFS is "," in this scope); the callers word-split on space.
	IFS=' '
	echo "${out[*]}"
}

# Resolve the three engine lists, folding the legacy single-engine flags in.
DB_RAW="${PERF_DB:-}"
[[ "${PERF_PG:-0}" == 1 ]] && DB_RAW="${DB_RAW:+${DB_RAW},}pg"
REMEASURE_RAW="${PERF_REMEASURE_DB:-}"
[[ "${PERF_REMEASURE_PG:-0}" == 1 ]] && REMEASURE_RAW="${REMEASURE_RAW:+${REMEASURE_RAW},}pg"
REUSE_ONLY_RAW="${PERF_REUSE_ONLY_DB:-}"
[[ "${PERF_PG_REUSE_ONLY:-0}" == 1 ]] && REUSE_ONLY_RAW="${REUSE_ONLY_RAW:+${REUSE_ONLY_RAW},}pg"
SEED_ONLY="${PERF_SEED_ONLY:-${PERF_PG_ONLY:+pg}}"
SEED_ONLY="${SEED_ONLY:-${PERF_EXT_ONLY:-}}"

DB_LIST="$(expand_engine_list "${DB_RAW}")"
REMEASURE_LIST="$(expand_engine_list "${REMEASURE_RAW}")"
REUSE_ONLY_LIST="$(expand_engine_list "${REUSE_ONLY_RAW}")"

# Membership test against a space-separated resolved list.
in_list() {
	local needle="$1" hay="$2" x
	for x in ${hay}; do [[ "${x}" == "${needle}" ]] && return 0; done
	return 1
}

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
if [[ "${SDB_DOCKER}" != 0 ]]; then
	command -v docker >/dev/null 2>&1 || {
		echo "docker not found (PERF_SDB_DOCKER=0 runs serened as a bare process)" >&2
		exit 1
	}
	docker image inspect "${SDB_IMAGE}" >/dev/null 2>&1 ||
		docker pull "${SDB_IMAGE}" >/dev/null
fi

mkdir -p "${OUT_DIR}"
DATA_OLD="$(mktemp -d /tmp/sdb_wire_old.XXXXXX)"
DATA_NEW="$(mktemp -d /tmp/sdb_wire_new.XXXXXX)"
CUR_PID=""
CUR_CONTAINER=""

cleanup() {
	[[ -n "${CUR_CONTAINER}" ]] && docker rm -f "${CUR_CONTAINER}" >/dev/null 2>&1 || true
	[[ -n "${CUR_PID}" && -z "${CUR_CONTAINER}" ]] && kill -9 "${CUR_PID}" 2>/dev/null || true
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
	# In docker mode the log file is only materialized on stop; pull it now.
	[[ -n "${CUR_CONTAINER}" ]] && docker logs "${CUR_CONTAINER}" >"${log}" 2>&1 || true
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
# Connection-lifecycle probe: the same trivial SELECT 1, but driven with pgbench -C
# (reconnect per transaction), so each "query" pays the full TCP connect + startup +
# auth handshake + teardown. The other workloads reuse connections and so never
# exercise that path; this isolates connection setup/teardown throughput.
WL[connect]="SELECT 1;"
for name in "${!WL[@]}"; do
	printf '%s\n' "${WL[$name]}" >"${OUT_DIR}/wl_${name}.sql"
done

# Serialization matrix: per-type x per-format (text/binary) bulk-drain
# throughput, the dimension pgbench cannot measure (text-only, and its row
# parsing caps the client near 400 MB/s). wire_serialize_drain.py drains raw
# bytes over the extended protocol with the result format set in Bind.
# Tables live in an attached duckdb-NATIVE file (rocksdb scan is slow,
# single-threaded and batch-index-less -- it would mask serialization);
# preserve_insertion_order=false puts the new server on the parallel
# (unordered) collector path. PERF_SERIALIZE=0 to skip.
SERIALIZE="${PERF_SERIALIZE:-1}"
SER_ROWS_PLAIN="${PERF_SER_ROWS_PLAIN:-5000000}"
SER_ROWS_NESTED="${PERF_SER_ROWS_NESTED:-2000000}"
SER_TYPES=(int double varchar json list struct nested)

serialize_setup() {
	local port="$1" datadir="$2"
	local db="${datadir}/serialize.duckdb"
	psql -h 127.0.0.1 -p "${port}" -U postgres -d postgres -AtX -q \
		-c "ATTACH '${db}' AS ser" \
		-c "CREATE TABLE IF NOT EXISTS ser.main.t_int AS SELECT i, i*1000000007 j, i%1000000 k, (i*31)%1000000000 l FROM range(${SER_ROWS_PLAIN}) t(i)" \
		-c "CREATE TABLE IF NOT EXISTS ser.main.t_double AS SELECT i/3.0 a, i/7.0 b, i/11.0 c, i/13.0 d FROM range(${SER_ROWS_PLAIN}) t(i)" \
		-c "CREATE TABLE IF NOT EXISTS ser.main.t_varchar AS SELECT 'user_'||i a, repeat('x', (16 + i%17)::int) b FROM range(${SER_ROWS_PLAIN}) t(i)" \
		-c "CREATE TABLE IF NOT EXISTS ser.main.t_json AS SELECT to_json(struct_pack(id := i, name := 'user_'||i, score := i/7.0)) j FROM range(${SER_ROWS_NESTED}) t(i)" \
		-c "CREATE TABLE IF NOT EXISTS ser.main.t_list AS SELECT [i, i+1, i+2, i+3] l FROM range(${SER_ROWS_NESTED}) t(i)" \
		-c "CREATE TABLE IF NOT EXISTS ser.main.t_struct AS SELECT struct_pack(a := i, b := i/3.0, c := 'user_'||i) s FROM range(${SER_ROWS_NESTED}) t(i)" \
		-c "CREATE TABLE IF NOT EXISTS ser.main.t_nested AS SELECT [struct_pack(k := i%10, v := [i, i+1]), struct_pack(k := (i+1)%10, v := [i*2])] n FROM range(${SER_ROWS_NESTED}/2) t(i)" \
		>/dev/null
}

# run_serialize <srv> <port>: one row per type x format. Columns reuse the
# results.tsv shape with tps=MB/s and lat=median seconds so the summary's
# ratio machinery applies untouched; a failed cell (e.g. a type without
# binary serialization) records 0 and the error lands in serialize_<srv>.log.
run_serialize() {
	local srv="$1" port="$2"
	local log="${OUT_DIR}/serialize_${srv}.log"
	if ! serialize_setup "${port}" "$3" >>"${log}" 2>&1; then
		echo "  serialize: setup failed (see ${log})" >&2
		return 0
	fi
	local typ fmt out
	for typ in "${SER_TYPES[@]}"; do
		for fmt in text binary; do
			out=$(python3 "${ROOT}/scripts/perf/wire_serialize_drain.py" "${port}" "${fmt}" 3 \
				"set preserve_insertion_order=false" \
				"SELECT * FROM ser.main.t_${typ}" \
				"${srv}_ser_${typ}_${fmt}" 2>>"${log}") || out="${srv}_ser_${typ}_${fmt}	0	0	0	ERROR: client failed"
			local label med mb mbps status
			IFS=$'\t' read -r label med mb mbps status <<<"${out}"
			printf '%s\t%s\t%s\t%s\t0\t0\n' "${label}" "${mbps}" "${med}" "${mb}" >>"${OUT_DIR}/results.tsv"
			printf '  %-26s %8s MB/s  (%ss, %s MB) %s\n' "${label}" "${mbps}" "${med}" "${mb}" "${status}"
			echo "${out}" >>"${log}"
		done
	done
}

# run_one <label> <port> <pid> <sqlfile> <pgbench-mode> [extra pgbench args]
# Returns nothing; appends a parsed row to ${OUT_DIR}/results.tsv.
run_one() {
	local label="$1" port="$2" pid="$3" sqlfile="$4" mode="$5"
	shift 5
	local extra=("$@")
	# External pg-wire engines connect as a different user/db (and CedarDB needs a
	# password via PGPASSWORD); RUN_ONE_USER/RUN_ONE_DB override the serened/pg
	# default of postgres/postgres. Empty = the original behavior, unchanged.
	local conn=(-h 127.0.0.1 -p "${port}" -U "${RUN_ONE_USER:-postgres}" "${RUN_ONE_DB:-postgres}")
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

# Start one serened (old- or new-wire flags alike). Docker mode mirrors
# bench_engine's conventions: --cpuset-cpus pinning to ENG_CPUSET, default bridge
# network with the port published, so the pgbench path through docker-proxy/NAT is
# identical to the external engines'. No image is built -- the host-compiled
# (statically linked) binary and the fresh datadir are bind-mounted at their HOST
# paths, so perf-record'ed mmap paths resolve to the real binary at report time.
# serened is the container's entrypoint (its init), so docker inspect .State.Pid is
# its pid in the HOST pid namespace; with --user it runs as the invoking uid, so
# perf stat/record attach to CUR_PID exactly as in the bare case and profiling
# stays fully functional in both modes.
start_serened() {
	local srv="$1" port="$2" datadir="$3"
	shift 3
	local cmd=("${SERENED_BIN}" "${datadir}" --server_io_threads "${IO_THREADS}")
	[[ -n "${CPU_THREADS}" ]] && cmd+=(--server_cpu_threads "${CPU_THREADS}")
	cmd+=("$@")
	if [[ "${SDB_DOCKER}" != 0 ]]; then
		local cpuset=()
		[[ -n "${ENG_CPUSET}" ]] && cpuset=(--cpuset-cpus "${ENG_CPUSET}")
		docker rm -f "${SDB_CONTAINER}" >/dev/null 2>&1 || true
		docker run -d --name "${SDB_CONTAINER}" "${cpuset[@]}" \
			--user "$(id -u):$(id -g)" -e HOME=/tmp \
			-p "${port}:${port}" \
			-v "${SERENED_BIN}:${SERENED_BIN}:ro" \
			-v "${datadir}:${datadir}" \
			"${SDB_IMAGE}" "${cmd[@]}" >/dev/null
		CUR_CONTAINER="${SDB_CONTAINER}"
		CUR_PID="$(docker inspect -f '{{.State.Pid}}' "${SDB_CONTAINER}")"
	else
		"${cmd[@]}" >"${OUT_DIR}/serened_${srv}.log" 2>&1 &
		CUR_PID=$!
	fi
}

# Kill the server started by start_serened; docker captured its output, so persist
# it to the same serened_<srv>.log the bare path writes directly.
stop_serened() {
	local srv="$1"
	if [[ -n "${CUR_CONTAINER}" ]]; then
		docker logs "${CUR_CONTAINER}" >"${OUT_DIR}/serened_${srv}.log" 2>&1 || true
		docker rm -f "${CUR_CONTAINER}" >/dev/null 2>&1 || true
		CUR_CONTAINER=""
	else
		kill -9 "${CUR_PID}" 2>/dev/null || true
		wait "${CUR_PID}" 2>/dev/null || true
	fi
	CUR_PID=""
}

# Benchmark ONE server fully, then kill it before the other is ever started, so
# the two never co-reside and an idle server's worker threads cannot pollute the
# measured one. This is the whole point of using separate processes.
bench_server() {
	local srv="$1" port="$2" datadir="$3"
	shift 3
	local how=bare
	[[ "${SDB_DOCKER}" != 0 ]] && how="docker${ENG_CPUSET:+, cpuset ${ENG_CPUSET}}"
	echo "-- ${srv} server (:${port}) [${how}; isolated; no other serened alive]${CPU_THREADS:+; cpu_threads=${CPU_THREADS}}"
	start_serened "${srv}" "${port}" "${datadir}" "$@"
	wait_up "${port}" "${OUT_DIR}/serened_${srv}.log"
	run_one "${srv}_select1_simple" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" simple
	run_one "${srv}_select1_extended" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" extended
	run_one "${srv}_select1_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" prepared
	run_one "${srv}_wide_simple" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_wide.sql" simple
	run_one "${srv}_wide_extended" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_wide.sql" extended
	run_one "${srv}_wide_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_wide.sql" prepared
	run_one "${srv}_rows_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_rows.sql" prepared
	run_one "${srv}_analytical_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_analytical.sql" prepared
	# Connection lifecycle: -C reconnects per transaction (simple protocol), so this
	# measures connect+startup+auth+teardown throughput rather than steady-state query cost.
	run_one "${srv}_connect_select1" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_connect.sql" simple -C
	if [[ "${SERIALIZE}" != 0 ]]; then
		echo "  -- serialization matrix (type x text/binary, raw drain)"
		run_serialize "${srv}" "${port}" "${datadir}"
	fi
	if [[ "${PROFILE}" != "0" ]]; then
		echo "  -- profiling ${srv} (perf record ${PROFILE_SECS}s/workload, still isolated)"
		profile_one "${srv}_select1_simple" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" simple
		profile_one "${srv}_select1_extended" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" extended
		profile_one "${srv}_select1_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_select1.sql" prepared
		profile_one "${srv}_wide_simple" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_wide.sql" simple
		profile_one "${srv}_wide_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_wide.sql" prepared
		profile_one "${srv}_rows_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_rows.sql" prepared
		profile_one "${srv}_analytical_prepared" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_analytical.sql" prepared
		# connect_select1 is intentionally not profiled: profile_one drives steady-state
		# (reused-connection) load and has no -C path, so a profile here would not reflect
		# the connect/teardown cost the workload measures.
	fi
	stop_serened "${srv}"
	sleep 1
}

# Measure ONE external pg-wire engine (any registry entry: pg / crdb / cedar /
# glare) in docker. No local pid to perf-stat, so tps/latency only; pinned to
# ENG_CPUSET so it races on serened's cores. The container listens on its native
# port (ENG_CPORT) which we publish on the host (ENG_HPORT); run_one connects to
# 127.0.0.1:HPORT as the engine's user/db, with any password via PGPASSWORD.
# Appends <name>_<workload> rows. Must NOT run inside the exclusive cgroup partition
# (the kernel denies the container the reserved cores) -- run_bench_isolated.sh
# seeds these caches outside the partition first.
bench_engine() {
	local name="$1"
	local image="${ENG_IMAGE[$name]}" hport="${ENG_HPORT[$name]}" cport="${ENG_CPORT[$name]}"
	local container="${ENG_CONTAINER[$name]}" user="${ENG_USER[$name]}" db="${ENG_DB[$name]}" pass="${ENG_PASS[$name]}"
	if ! command -v docker >/dev/null 2>&1; then
		echo "  ${name}: docker not found -- skipping" >&2
		return 1
	fi
	local cpuset=()
	[[ -n "${ENG_CPUSET}" ]] && cpuset=(--cpuset-cpus "${ENG_CPUSET}")
	# Word-split the per-engine docker pre-image args and container start args.
	# (read returns non-zero on the empty/no-trailing-newline here-string; harmless.)
	local preargs=() cargs=()
	read -r -a preargs <<<"${ENG_PREARGS[$name]}" || true
	read -r -a cargs <<<"${ENG_CARGS[$name]}" || true
	echo "-- ${name} ${image} (host :${hport} -> container :${cport}) [docker${ENG_CPUSET:+, cpuset ${ENG_CPUSET}}]"
	docker rm -f "${container}" >/dev/null 2>&1 || true
	if ! docker run -d --name "${container}" "${cpuset[@]}" \
		-p "${hport}:${cport}" "${preargs[@]}" "${image}" "${cargs[@]}" >/dev/null; then
		echo "  ${name}: docker run failed -- skipping" >&2
		return 1
	fi
	local ready_uri="postgres://${user}:${pass}@127.0.0.1:${hport}/${db}?sslmode=disable"
	local up=0
	for _ in $(seq 1 90); do
		if PGPASSWORD="${pass}" psql "${ready_uri}" -c 'SELECT 1' >/dev/null 2>&1; then
			up=1
			break
		fi
		sleep 1
	done
	if [[ "${up}" != 1 ]]; then
		echo "  ${name} did not come up:" >&2
		docker logs "${container}" 2>&1 | tail -20 >&2
		docker rm -f "${container}" >/dev/null 2>&1 || true
		return 1
	fi
	# Same workload set + layout as bench_server, as <name>_<workload> rows. The
	# differing user/db/password reach pgbench via run_one's RUN_ONE_USER/RUN_ONE_DB
	# overrides + PGPASSWORD.
	local wl
	export RUN_ONE_USER="${user}" RUN_ONE_DB="${db}" PGPASSWORD="${pass}"
	for wl in select1:simple select1:extended select1:prepared \
		wide:simple wide:extended wide:prepared \
		rows:prepared analytical:prepared; do
		run_one "${name}_${wl/:/_}" "${hport}" "-" "${OUT_DIR}/wl_${wl%%:*}.sql" "${wl##*:}"
	done
	run_one "${name}_connect_select1" "${hport}" "-" "${OUT_DIR}/wl_connect.sql" simple -C
	unset RUN_ONE_USER RUN_ONE_DB PGPASSWORD
	docker rm -f "${container}" >/dev/null 2>&1 || true
}

mkdir -p "${BASELINE_DIR}"

# Single-engine seeding mode: measure + cache ONE engine, then exit (run outside the
# cgroup partition so the container can use the reserved cores). run_bench_isolated.sh
# uses it to seed each enabled engine before reserving the partition.
if [[ -n "${SEED_ONLY}" ]]; then
	if [[ -z "${ENG_IMAGE[$SEED_ONLY]+x}" ]]; then
		echo "PERF_SEED_ONLY='${SEED_ONLY}' is not a known engine (${EXT_ORDER[*]})" >&2
		exit 1
	fi
	echo "=== ${SEED_ONLY}-only: measuring + caching baseline ==="
	if bench_engine "${SEED_ONLY}"; then
		grep "^${SEED_ONLY}_" "${OUT_DIR}/results.tsv" >"${BASELINE_DIR}/${SEED_ONLY}.tsv"
		echo "cached ${SEED_ONLY} baseline -> ${BASELINE_DIR}/${SEED_ONLY}.tsv"
		exit 0
	fi
	echo "${SEED_ONLY} baseline NOT cached (measurement failed)" >&2
	exit 1
fi

echo
echo "=== benchmarking (old + selected engines cached/reused; new always measured) ==="
[[ -n "${DB_LIST}" ]] && echo "    engines: ${DB_LIST}"
# OLD -- same binary as new, so cache and reuse across new-server iterations.
if [[ -s "${OLD_CACHE}" && "${REMEASURE_OLD}" != 1 ]]; then
	echo "-- old: reusing cached baseline (${OLD_CACHE}); PERF_REMEASURE_OLD=1 to refresh"
	cat "${OLD_CACHE}" >>"${OUT_DIR}/results.tsv"
else
	bench_server old "${PORT_OLD}" "${DATA_OLD}" --server_endpoints "pgsql+tcp://0.0.0.0:${PORT_OLD}"
	grep '^old_' "${OUT_DIR}/results.tsv" >"${OLD_CACHE}" && echo "  cached old baseline -> ${OLD_CACHE}"
fi
# EXTERNAL ENGINES (pg / crdb / cedar / glare) -- whichever PERF_DB selected, all
# cached identically. reuse-only (set inside the partition) forces using the seeded
# cache, ignoring remeasure (the container can't schedule on the reserved cores).
for eng in ${DB_LIST}; do
	cache="${BASELINE_DIR}/${eng}.tsv"
	reuse_only=0
	in_list "${eng}" "${REUSE_ONLY_LIST}" && reuse_only=1
	remeasure=0
	in_list "${eng}" "${REMEASURE_LIST}" && remeasure=1
	if [[ -s "${cache}" && ("${remeasure}" != 1 || "${reuse_only}" == 1) ]]; then
		echo "-- ${eng}: reusing cached baseline (${cache}); PERF_REMEASURE_DB=${eng} to refresh"
		cat "${cache}" >>"${OUT_DIR}/results.tsv"
	elif [[ "${reuse_only}" == 1 ]]; then
		echo "-- ${eng}: reuse-only and no cache present -- skipping ${eng} column" >&2
	elif bench_engine "${eng}"; then
		grep "^${eng}_" "${OUT_DIR}/results.tsv" >"${cache}" && echo "  cached ${eng} baseline -> ${cache}"
	fi
done
# NEW -- always measured.
bench_server new "${PORT_NEW}" "${DATA_NEW}" --network_pg_endpoint "pgsql://0.0.0.0:${PORT_NEW}"

# Summary table: old vs new per workload (+ one tps/ratio column per selected
# engine, in registry order), with cycles-per-query. The engine order is passed in
# so the columns match the registry; engines absent from the data are dropped.
echo
echo "================== WIRE BENCH SUMMARY =================="
echo "clients=${CLIENTS} jobs=${JOBS} dur=${DURATION}s io_threads=${IO_THREADS}"
echo
awk -F'\t' -v engorder="${EXT_ORDER[*]}" '
{
  # First underscore-delimited token is the side (old/new/pg/crdb/...); the rest is
  # the workload key. All external engines are handled uniformly here, else their
  # prefixed rows would be misread as new_ and corrupt the table.
  row=$1; p=index(row,"_"); side=substr(row,1,p-1); key=substr(row,p+1);
  if (side!="old" && side!="new") have[side]=1;
  tps[key,side]=$2; lat[key,side]=$3; txn[key,side]=$4; cyc[key,side]=$5;
  if (!(key in seen)) { order[++n]=key; seen[key]=1 }
}
END {
  # Engine columns: registry order, only those present in the data.
  ne=0; nc=split(engorder,cand," ");
  for (c=1;c<=nc;c++) if (have[cand[c]]) ext[++ne]=cand[c];

  printf "%-20s %12s %12s","workload","tps(old)","tps(new)";
  for (e=1;e<=ne;e++) printf " %12s","tps("ext[e]")";
  printf " %10s",  "new/old";
  for (e=1;e<=ne;e++) printf " %10s","new/"ext[e];
  printf " %14s %14s\n","cyc/q(old)","cyc/q(new)";
  printf "%-20s %12s %12s","--------------------","------------","------------";
  for (e=1;e<=ne;e++) printf " %12s","------------";
  printf " %10s","----------";
  for (e=1;e<=ne;e++) printf " %10s","----------";
  printf " %14s %14s\n","--------------","--------------";

  for (i=1;i<=n;i++) {
    k=order[i];
    to=tps[k,"old"]+0; tn=tps[k,"new"]+0;
    rno=(to>0)?sprintf("%.2fx",tn/to):"n/a";
    cqo=(txn[k,"old"]+0>0)?sprintf("%.0f",cyc[k,"old"]/txn[k,"old"]):"n/a";
    cqn=(txn[k,"new"]+0>0)?sprintf("%.0f",cyc[k,"new"]/txn[k,"new"]):"n/a";
    printf "%-20s %12.1f %12.1f",k,to,tn;
    for (e=1;e<=ne;e++) printf " %12.1f",tps[k,ext[e]]+0;
    printf " %10s",rno;
    for (e=1;e<=ne;e++) { te=tps[k,ext[e]]+0; printf " %10s",(te>0)?sprintf("%.2fx",tn/te):"n/a" }
    printf " %14s %14s\n",cqo,cqn;
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
