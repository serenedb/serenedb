#!/usr/bin/env bash
# bench_wire_old_vs_new.sh -- transport/protocol benchmark: the OLD pg-wire
# stack (pg_comm_task + general_server, --listen) vs the NEW one
# (server/network/, --listen), from the SAME binary.
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
# Every comparison engine -- the old serenedb release, real PostgreSQL, crdb,
# ... -- runs in docker and is a peer: measured once, cached, and reused across
# new-server iterations (caches persist outside the timestamped run dir).
# Selection + refresh are uniform via PERF_DB / PERF_REMEASURE_DB; there are no
# engine-specific flags. Only `new` (the local binary under test) is measured
# every run.
BASELINE_DIR="${PERF_BASELINE_DIR:-${RESULTS_DIR}/baselines}"
# Cores to pin the external (docker) engines to, so they race on the same CPUs as
# serened. run_bench_isolated.sh exports the reserved set here; empty = all cores.
ENG_CPUSET="${PERF_BENCH_CORES:-}"
# Dedicated core for the single-threaded drain/copy clients (exported by
# run_bench_isolated.sh). Unpinned they race the server workers for the same
# cores and the ser/copy cells go bimodal (~4 vs ~9 GB/s on scheduling luck).
CLIENT_PIN=()
[[ -n "${PERF_CLIENT_CORES:-}" ]] && CLIENT_PIN=(taskset -c "${PERF_CLIENT_CORES}")
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

# Host networking for ALL containers (serened + engines). docker's default
# published-port path goes through the userland docker-proxy, which (a) caps
# connect-per-second around ~5k for every engine (the connect_select1 ceiling)
# and (b) holds one host ephemeral port per inbound connection in TIME_WAIT --
# a -C storm burns the whole 28k range in seconds and resets everything for
# ~60s. With --network host every engine binds the bench port directly, so
# connect numbers measure the engines, not the proxy. Still fair: all engines
# share the mode. PERF_DOCKER_HOSTNET=0 restores proxy networking. Engine
# baselines are NOT comparable across modes -- refresh with
# PERF_REMEASURE_DB=all after switching.
DOCKER_HOSTNET="${PERF_DOCKER_HOSTNET:-1}"

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
EXT_ORDER=(old pg crdb cedar risingwave clickhouse) # display + iteration order; old (last serenedb release) leftmost
declare -A ENG_IMAGE ENG_HPORT ENG_CPORT ENG_CONTAINER ENG_USER ENG_DB ENG_PASS
declare -A ENG_PREARGS ENG_CARGS # docker args before the image / container start args after it
# Host-networking variants: full REPLACEMENT start/env args that make the
# engine listen on ENG_HPORT directly (no -p mapping exists under --network
# host). An engine with no ENG_CARGS_HOST entry keeps its base ENG_CARGS and
# its NATIVE port (the bench then connects to ENG_CPORT). Auxiliary listeners
# are moved to 1<hport> / 2<hport> so well-known ports (8080, 8123, 9000)
# don't collide with co-tenants on the shared host.
declare -A ENG_CARGS_HOST ENG_PREARGS_HOST

# Previous serenedb RELEASE -- the honest "old" baseline: the last published
# server image (old pg-wire stack), NOT the same local binary with the (removed)
# --listen path. Trust auth, pg wire on its native 7890; with
# --network host it binds 7890 directly (no host-port override), so it is treated
# exactly like every other engine -- selected via PERF_DB, never profiled.
ENG_IMAGE[old]="${PERF_OLD_IMAGE:-serenedb/serenedb:latest}"
ENG_HPORT[old]="${PERF_OLD_PORT:-7890}"
ENG_CPORT[old]=7890
ENG_CONTAINER[old]="${PERF_OLD_CONTAINER:-sdb_wire_old}"
ENG_USER[old]=postgres
ENG_DB[old]=postgres
ENG_PASS[old]=""
ENG_PREARGS[old]=""
ENG_CARGS[old]=""
# No ENG_CARGS_HOST[old]: keep the image's native 7890 listener under host net.

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
ENG_CARGS_HOST[pg]="${ENG_CARGS[pg]} -c port=${ENG_HPORT[pg]}"

# CockroachDB: insecure single-node, SQL on 26257, user root / db defaultdb / no pw.
ENG_IMAGE[crdb]="${PERF_CRDB_IMAGE:-cockroachdb/cockroach:latest}"
# Recent cockroach hard-requires listen_addr = (127.0.0.1|localhost):26257 -- it
# refuses both a 0.0.0.0 hostname and any non-26257 port -- so crdb cannot be
# relocated off its native SQL port. Bind 26257 directly (host == container port)
# and only move the HTTP UI off 8080. The bench connects to 26257.
ENG_HPORT[crdb]="${PERF_CRDB_PORT:-26257}"
ENG_CPORT[crdb]=26257
ENG_CONTAINER[crdb]="${PERF_CRDB_CONTAINER:-sdb_wire_crdb}"
ENG_USER[crdb]=root
ENG_DB[crdb]=defaultdb
ENG_PASS[crdb]=""
ENG_PREARGS[crdb]=""
ENG_CARGS[crdb]="start-single-node --insecure"
ENG_CARGS_HOST[crdb]="${ENG_CARGS[crdb]} --listen-addr=localhost:${ENG_HPORT[crdb]} --http-addr=localhost:${PERF_CRDB_HTTP_PORT:-18080}"

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
ENG_PREARGS_HOST[cedar]="${ENG_PREARGS[cedar]} -e CEDARDB_PORT=${ENG_HPORT[cedar]}"
ENG_CARGS_HOST[cedar]=""

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
ENG_CARGS_HOST[risingwave]="single_node --listen-addr 0.0.0.0:${ENG_HPORT[risingwave]}"

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
ENG_CARGS_HOST[clickhouse]="--postgresql_port=${ENG_HPORT[clickhouse]} --http_port=1${ENG_HPORT[clickhouse]} --tcp_port=2${ENG_HPORT[clickhouse]}"

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

# Resolve the engine lists. Every engine (old serenedb, pg, crdb, ...) is
# selected uniformly through these -- no per-engine flags.
DB_RAW="${PERF_DB:-}"
REMEASURE_RAW="${PERF_REMEASURE_DB:-}"
REUSE_ONLY_RAW="${PERF_REUSE_ONLY_DB:-}"
SEED_ONLY="${PERF_SEED_ONLY:-}"

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
DATA_NEW="$(mktemp -d /tmp/sdb_wire_new.XXXXXX)"
CUR_PID=""
CUR_CONTAINER=""

cleanup() {
	[[ -n "${CUR_CONTAINER}" ]] && docker rm -f "${CUR_CONTAINER}" >/dev/null 2>&1 || true
	[[ -n "${CUR_PID}" && -z "${CUR_CONTAINER}" ]] && kill -9 "${CUR_PID}" 2>/dev/null || true
	rm -rf "${DATA_NEW}"
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

# --- per-workload peak RSS sampler -----------------------------------------
# VmHWM in /proc is monotonic over the whole process life, so it cannot give a
# PER-workload peak. Instead poll VmRSS in a tight background loop during the
# run and keep the running max (process-wide -- the worker threads share the
# address space, so VmRSS already covers them). Cheap enough not to perturb
# throughput. Only the bare/local `new` server has a host pid we can read
# /proc for; docker (CUR_CONTAINER set) and the external engines have no usable
# local pid here, so the sampler degrades to "-" and the column shows it.
#
# rss_sampler_start <pid> <outfile>: print the sampler's pid on stdout (empty
# when not sampling) and seed <outfile> with the peak (kB) or "-". The
# background subshell MUST redirect its stdout, else the $(...) that captures
# the sampler pid would block until the (long-lived) sampler exits.
rss_sampler_start() {
	local pid="$1" outfile="$2"
	if [[ -z "${pid}" || "${pid}" == "-" || ! -r "/proc/${pid}/status" ]]; then
		printf '%s\n' "-" >"${outfile}"
		echo ""
		return
	fi
	(
		max=0
		while :; do
			# A vanished pid (server killed / container exit) ends sampling; the
			# last max already in <outfile> stands.
			cur=$(awk '/^VmRSS:/{print $2; exit}' "/proc/${pid}/status" 2>/dev/null) || break
			[[ -z "${cur}" ]] && break
			((cur > max)) && max=${cur}
			printf '%s\n' "${max}" >"${outfile}"
			sleep 0.05
		done
	) >/dev/null 2>&1 &
	echo "$!"
}

# rss_sampler_stop <sampler-pid>: end the loop (the peak is already in the
# outfile). Empty pid (not sampling) is a no-op.
rss_sampler_stop() {
	local sp="$1"
	[[ -z "${sp}" ]] && return
	kill "${sp}" 2>/dev/null || true
	wait "${sp}" 2>/dev/null || true
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
# Comparison engines that also get the serialize matrix. The setup is now plain
# postgres-compatible CREATE TABLE, so every engine populates its own real
# tables; an engine that rejects a type (no composite, bad pg-compat) just
# records 0 for that cell. Defaults to all engines; space list, "" disables.
SER_ENGINES="${PERF_SER_ENGINES:-${EXT_ORDER[*]}}"
# Escaping workloads: varchar/list/struct/nested whose strings carry tab (=delim),
# newline, backslash and double-quote (built with chr() so the SQL has no
# shell-special bytes). These exercise the run-batch escape path (flush run + emit
# escape) instead of the all-clean bulk path -- measured text + copy_text only
# (binary is length-prefixed, escape-independent).
SER_ESC_TYPES=(varchar list struct nested)
# Extra new-server serialize/copy-out columns: new(op) = order-preserving
# collector (preserve_insertion_order=true), new(t1) = single-threaded (runtime
# SET threads=1, restored after). PERF_SER_VARIANTS=0 to measure only the
# default parallel-unordered column.
SER_VARIANTS="${PERF_SER_VARIANTS:-1}"

# COPY FROM STDIN ingest matrix (capture-replay, per type x {binary,text,csv}).
# Ingest is insert/append-bound -- far slower than the copy-out drain -- so it
# uses a small dedicated source table (COPYIN_ROWS, parallel CTAS, no LIMIT) and
# is single-threaded by nature (no order/threads variants). csv is the reference
# format (PG's bulk-load path); the goal is to beat PG on text+binary too.
# PERF_COPYIN=0 to skip.
COPYIN="${PERF_COPYIN:-1}"
COPYIN_ROWS="${PERF_COPYIN_ROWS:-200000}"
COPYIN_FRAME="${PERF_COPYIN_FRAME:-262144}"
COPYIN_REPS="${PERF_COPYIN_REPS:-5}"
SER_REPS="${PERF_SER_REPS:-3}"
COPYIN_FMTS=(binary text csv)

# All serialize setup connects as the engine's trust user/db and is
# failure-tolerant (|| true): an engine that rejects a statement (poor pg
# compatibility, no composite types, ...) just leaves the table absent, and
# run_serialize's drain records 0 for it -- exactly like old's unsupported
# binary-copy cells.
ser_psql() { psql -h 127.0.0.1 -p "$1" -U "${RUN_ONE_USER:-postgres}" -d "${RUN_ONE_DB:-postgres}" -AtX -q "${@:2}" >/dev/null 2>&1 || true; }
# ser_psql_q <port> <sql>: same connection, but captures the scalar result (used
# to read/restore the global `threads` setting around the new(t1) variant).
ser_psql_q() { psql -h 127.0.0.1 -p "$1" -U "${RUN_ONE_USER:-postgres}" -d "${RUN_ONE_DB:-postgres}" -AtX -q -c "$2" 2>/dev/null; }
# ser_mk <port> <table> <select>: plain CREATE TABLE AS SELECT.
ser_mk() { ser_psql "$1" -c "DROP TABLE IF EXISTS $2" -c "CREATE TABLE $2 AS $3"; }
# ser_mk_typed <port> <table> <type> <type-def> <select>: struct/nested need a
# NAMED composite type -- an anonymous ROW cannot be a table column. CREATE TYPE
# ... AS (...) + ROW(...)::type is the portable pg form (works on PG + serenedb;
# engines without user composite types reject it -> the cell records 0).
ser_mk_typed() {
	ser_psql "$1" -c "DROP TABLE IF EXISTS $2" -c "DROP TYPE IF EXISTS $3 CASCADE" \
		-c "CREATE TYPE $3 AS ($4)" -c "CREATE TABLE $2 AS $5"
}

# Real native tables (no duckdb ATTACH), generated the postgres way so every
# engine populates its own real tables. generate_series is the portable row
# source; serial on some engines, but this is one-time setup, not the measured
# drain. json is built as text then cast (::json) since json_build_object is not
# universal; struct/nested use composite types (above).
# Build the per-type source tables at <P>/<N> rows with name suffix <s>, via a
# parallel CTAS (NOT a LIMIT -- LIMIT forces a single-threaded scan). Reused for
# the big serialize/copy-out tables (suffix "") and the small copy-in source
# tables (suffix "_cin", COPYIN_ROWS rows).
serialize_base_tables() {
	local port="$1" P="$2" N="$3" s="$4"
	ser_mk "${port}" "t_int${s}" "SELECT i, i*1000000007 AS j, i%1000000 AS k, (i*31)%1000000000 AS l FROM generate_series(1::bigint, ${P}::bigint) g(i)"
	ser_mk "${port}" "t_double${s}" "SELECT i::float8/3 AS a, i::float8/7 AS b, i::float8/11 AS c, i::float8/13 AS d FROM generate_series(1, ${P}) g(i)"
	ser_mk "${port}" "t_varchar${s}" "SELECT 'user_'||i AS a, repeat('x', (16 + i%17)::int) AS b FROM generate_series(1, ${P}) g(i)"
	ser_mk "${port}" "t_json${s}" "SELECT ('{\"id\":'||i||',\"name\":\"user_'||i||'\",\"score\":'||(i::float8/7)||'}')::json AS j FROM generate_series(1, ${N}) g(i)"
	ser_mk "${port}" "t_list${s}" "SELECT ARRAY[i, i+1, i+2, i+3] AS l FROM generate_series(1, ${N}) g(i)"
	ser_mk_typed "${port}" "t_struct${s}" "ser_struct${s}" "a bigint, b float8, c text" \
		"SELECT ROW(i, i::float8/3, 'user_'||i)::ser_struct${s} AS s FROM generate_series(1, ${N}) g(i)"
	ser_mk_typed "${port}" "t_nested${s}" "ser_nested${s}" "k int, v int[]" \
		"SELECT ARRAY[ROW(i%10, ARRAY[i, i+1])::ser_nested${s}, ROW((i+1)%10, ARRAY[i*2])::ser_nested${s}] AS n FROM generate_series(1, ${N}/2) g(i)"
}

serialize_setup() {
	local port="$1"
	serialize_base_tables "${port}" "${SER_ROWS_PLAIN}" "${SER_ROWS_NESTED}" ""
	# Escaping workloads (text formats only) -- serialize/copy-out matrix only.
	ser_mk "${port}" t_varchar_esc "SELECT 'row_'||i||chr(9)||'col'||chr(10)||'w'||chr(92)||'b'||chr(34)||'q'||(i%97) AS v FROM generate_series(1, ${SER_ROWS_NESTED}) g(i)"
	ser_mk "${port}" t_list_esc "SELECT ARRAY['a'||chr(34)||(i%13), chr(9)||'c'||(i%7), 'd'||chr(92)||'e'] AS l FROM generate_series(1, ${SER_ROWS_NESTED}) g(i)"
	ser_mk_typed "${port}" t_struct_esc ser_struct_esc "a text, b text, c text" \
		"SELECT ROW('x'||chr(34)||i, chr(9)||'z', 'w'||chr(92)||'v')::ser_struct_esc AS s FROM generate_series(1, ${SER_ROWS_NESTED}) g(i)"
	ser_mk_typed "${port}" t_nested_esc ser_nested_esc "k text, v text[]" \
		"SELECT ARRAY[ROW('p'||chr(34)||(i%10), ARRAY['q'||chr(9)||'r'||(i%5), 's'||chr(92)||'t'])::ser_nested_esc] AS n FROM generate_series(1, ${SER_ROWS_NESTED}/2) g(i)"
}

# Small per-type source tables for the copy-in matrix (parallel CTAS, no LIMIT).
copyin_setup() {
	serialize_base_tables "$1" "${COPYIN_ROWS}" "${COPYIN_ROWS}" "_cin"
}

# run_serialize <srv> <port>: one row per type x format. Columns reuse the
# results.tsv shape with tps=MB/s and lat=median seconds so the summary's
# ratio machinery applies untouched; a failed cell (e.g. a type without
# binary serialization) records 0 and the error lands in serialize_<srv>.log.
# Interleaved-rep accounting shared by the serialize and copy-in matrices: the
# timing pass runs the whole matrix REPS times, ONE rep per cell per pass, and
# takes the per-cell median at the end. Sequential per-cell reps let slow drift
# on the box bias whole cells (the matrices were bimodal run-to-run); with
# interleaving the drift lands on every cell equally and the median cancels it.
# cell_record <label> <one-rep-output> <log>: accumulate one pass of one cell.
cell_record() {
	local lbl="$1" out="$2" log="$3"
	local label med mb mbps status
	IFS=$'\t' read -r label med mb mbps status <<<"${out}"
	echo "${out}" >>"${log}"
	if [[ -z "${CELL_SEEN[${lbl}]:-}" ]]; then
		CELL_SEEN["${lbl}"]=1
		CELL_ORDER+=("${lbl}")
	fi
	CELL_TIMES["${lbl}"]+=" ${med}"
	CELL_MB["${lbl}"]="${mb}"
	CELL_STATUS["${lbl}"]="${status}"
}

# cell_emit <printf-width>: per cell, median of the recorded times -> one
# results.tsv row + one pretty line, in first-seen (matrix) order.
cell_emit() {
	local width="$1" lbl med mbps
	for lbl in "${CELL_ORDER[@]}"; do
		med=$(printf '%s\n' ${CELL_TIMES[${lbl}]} | sort -g | awk '{a[NR] = $1} END {print a[int((NR + 1) / 2)]}')
		mbps=$(awk -v mb="${CELL_MB[${lbl}]}" -v med="${med}" 'BEGIN {if (med > 0) printf "%d", mb / med; else printf "0"}')
		printf '%s\t%s\t%s\t%s\t0\t0\n' "${lbl}" "${mbps}" "${med}" "${CELL_MB[${lbl}]}" >>"${OUT_DIR}/results.tsv"
		printf "  %-${width}s %8s MB/s  (%ss, %s MB) %s\n" "${lbl}" "${mbps}" "${med}" "${CELL_MB[${lbl}]}" \
			"${CELL_STATUS[${lbl}]}"
	done
}

# ser_measure <log> <port> <label> <drain-mode> <query>: ONE rep of one
# type x format cell into the cell accumulator (failures -> 0, never abort).
ser_measure() {
	local log="$1" port="$2" lbl="$3" mode="$4" query="$5" out
	# Profiling pass (PROFILE_DRAIN_PID armed): record the server pid into a
	# per-cell perf.data instead of measuring -- one file per cell/variant so each
	# can be `perf annotate`d independently. No results.tsv row (the clean
	# measurement pass already recorded it).
	if [[ -n "${PROFILE_DRAIN_PID:-}" ]]; then
		perf record -g -o "${OUT_DIR}/perf_${lbl}.data" -p "${PROFILE_DRAIN_PID}" -- \
			"${CLIENT_PIN[@]}" python3 "${ROOT}/scripts/perf/wire_serialize_drain.py" "${port}" "${mode}" 3 \
			"${SER_SETUP}" "${query}" "${lbl}" >/dev/null 2>&1 || true
		return
	fi
	out=$("${CLIENT_PIN[@]}" python3 "${ROOT}/scripts/perf/wire_serialize_drain.py" "${port}" "${mode}" 1 \
		"${SER_SETUP}" "${query}" "${lbl}" 2>>"${log}") ||
		out="${lbl}	0	0	0	ERROR: client failed"
	cell_record "${lbl}" "${out}" "${log}"
}

# run_serialize_matrix <variant> <port> <setup>: one row per type x format under
# the given variant label prefix; ser_measure reads SER_SETUP via dynamic scope.
run_serialize_matrix() {
	local srv="$1" port="$2" SER_SETUP="$3"
	local log="${OUT_DIR}/serialize_${srv%%_*}.log"
	local typ fmt lbl pass reps="${SER_REPS}"
	local -A CELL_TIMES=() CELL_MB=() CELL_STATUS=() CELL_SEEN=()
	local -a CELL_ORDER=()
	[[ -n "${PROFILE_DRAIN_PID:-}" ]] && reps=1
	for ((pass = 1; pass <= reps; pass++)); do
		for typ in "${SER_TYPES[@]}"; do
			for fmt in text binary copy_text copy_binary; do
				case "${fmt}" in
				copy_*) lbl="${srv}_copy_${typ}_${fmt#copy_}" ;;
				*) lbl="${srv}_ser_${typ}_${fmt}" ;;
				esac
				ser_measure "${log}" "${port}" "${lbl}" "${fmt}" "SELECT * FROM t_${typ}"
			done
		done
		# Escaping workloads (text formats only): copy_text vs ser_text on data with
		# tab/newline/backslash/quote -- exercises the run-batch escape path.
		for typ in "${SER_ESC_TYPES[@]}"; do
			ser_measure "${log}" "${port}" "${srv}_ser_${typ}_esc_text" text \
				"SELECT * FROM t_${typ}_esc"
			ser_measure "${log}" "${port}" "${srv}_copy_${typ}_esc_text" copy_text \
				"SELECT * FROM t_${typ}_esc"
		done
	done
	[[ -z "${PROFILE_DRAIN_PID:-}" ]] && cell_emit 30
}

run_serialize() {
	local srv="$1" port="$2"
	local log="${OUT_DIR}/serialize_${srv}.log"
	# Setup is skipped in the profiling pass (tables already exist from the
	# measurement pass); each CREATE is independent and a missing table's drain
	# records 0, so never hard-stop on setup.
	[[ -z "${PROFILE_DRAIN_PID:-}" ]] && serialize_setup "${port}" >>"${log}" 2>&1
	# preserve_insertion_order=false puts serenedb on the parallel (unordered)
	# collector path; it is a duckdb-only setting, so apply it ONLY to
	# serenedb-lineage engines -- an unknown SET fails another engine's drain
	# entirely (the whole connection errors before the result streams).
	if [[ "${srv}" == new ]]; then
		run_serialize_matrix new "${port}" "set preserve_insertion_order=false"
		if [[ "${SER_VARIANTS}" != 0 ]]; then
			run_serialize_matrix new_op "${port}" "set preserve_insertion_order=true"
			# new(t1) = forced single thread. OFF by default: it tracks new(op)
			# within noise (order-preserving already drives the collector's Direct
			# single-sink path, i.e. single-threaded encode), so it just adds a
			# redundant table. PERF_SER_T1=1 to re-enable. threads is a GLOBAL
			# scheduler knob, so set it once over the wire, run, then restore.
			if [[ "${PERF_SER_T1:-0}" != 0 ]]; then
				local orig_threads
				orig_threads=$(ser_psql_q "${port}" "SELECT current_setting('threads')")
				ser_psql "${port}" -c "SET threads=1"
				run_serialize_matrix new_t1 "${port}" "set preserve_insertion_order=false"
				if [[ -n "${orig_threads}" ]]; then
					ser_psql "${port}" -c "SET threads=${orig_threads}"
				else
					ser_psql "${port}" -c "RESET threads"
				fi
			fi
		fi
	elif [[ "${srv}" == old ]]; then
		run_serialize_matrix old "${port}" "set preserve_insertion_order=false"
	else
		run_serialize_matrix "${srv}" "${port}" ""
	fi
}

# copyin_measure <log> <port> <label> <src-table> <fmt>: capture-replay one
# copy-in cell, record MB/s (failures -> 0, never abort). Profile-aware like
# ser_measure (per-cell perf.data in the profiling pass).
copyin_measure() {
	local log="$1" port="$2" lbl="$3" src="$4" fmt="$5" out
	if [[ -n "${PROFILE_DRAIN_PID:-}" ]]; then
		perf record -g -o "${OUT_DIR}/perf_${lbl}.data" -p "${PROFILE_DRAIN_PID}" -- \
			"${CLIENT_PIN[@]}" python3 "${ROOT}/scripts/perf/wire_copy_in.py" "${port}" "${src}" "${fmt}" \
			"${COPYIN_REPS}" "${COPYIN_FRAME}" "${lbl}" >/dev/null 2>&1 || true
		return
	fi
	out=$("${CLIENT_PIN[@]}" python3 "${ROOT}/scripts/perf/wire_copy_in.py" "${port}" "${src}" "${fmt}" \
		1 "${COPYIN_FRAME}" "${lbl}" 2>>"${log}") ||
		out="${lbl}	0	0	0	ERROR: client failed"
	cell_record "${lbl}" "${out}" "${log}"
}

run_copyin() {
	local srv="$1" port="$2"
	local log="${OUT_DIR}/copyin_${srv}.log"
	[[ -z "${PROFILE_DRAIN_PID:-}" ]] && copyin_setup "${port}" >>"${log}" 2>&1
	local typ fmt pass reps="${COPYIN_REPS}"
	local -A CELL_TIMES=() CELL_MB=() CELL_STATUS=() CELL_SEEN=()
	local -a CELL_ORDER=()
	[[ -n "${PROFILE_DRAIN_PID:-}" ]] && reps=1
	for ((pass = 1; pass <= reps; pass++)); do
		for typ in "${SER_TYPES[@]}"; do
			for fmt in "${COPYIN_FMTS[@]}"; do
				copyin_measure "${log}" "${port}" "${srv}_copyin_${typ}_${fmt}" "t_${typ}_cin" "${fmt}"
			done
		done
	done
	[[ -z "${PROFILE_DRAIN_PID:-}" ]] && cell_emit 34
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
	local rss_log="${OUT_DIR}/rss_${label}.txt"

	# Warm the connections/JIT before the measured window.
	pgbench "${conn[@]}" -n -f "${sqlfile}" -M "${mode}" \
		-c "${CLIENTS}" -j "${JOBS}" -T 2 "${extra[@]}" \
		>/dev/null 2>&1 || true

	pgbench "${conn[@]}" -n -f "${sqlfile}" -M "${mode}" \
		-c "${CLIENTS}" -j "${JOBS}" -T "${DURATION}" -r "${extra[@]}" \
		>"${pgb_log}" 2>&1 &
	local pgb_pid=$!
	# Peak-RSS sampling: poll /proc/<pid>/status VmRSS across the measured window.
	# `pid` is the server's host-visible pid for BOTH the bare server and the
	# docker new-server (docker inspect .State.Pid -- serened is the container
	# init, so its container-PID-1 maps to this host pid). External engines pass
	# pid="-", where rss_sampler_start is a no-op and the cell shows "-".
	local rss_pid=""
	rss_pid="$(rss_sampler_start "${pid}" "${rss_log}")"
	# Sample the server for slightly less than the pgbench window so perf stops
	# while load is still flowing (clean steady-state counters). External engines
	# (postgres in docker) have no local pid to attach to -> tps/latency only.
	if [[ -n "${pid}" && "${pid}" != "-" ]]; then
		perf stat -p "${pid}" -e task-clock,cycles,instructions \
			-o "${stat_log}" -- sleep "$((DURATION - 2))" >/dev/null 2>&1 || true
	else
		sleep "$((DURATION - 2))"
	fi
	rss_sampler_stop "${rss_pid}"
	wait "${pgb_pid}" 2>/dev/null || true

	local tps lat cycles insns txns rss_kb rss_mb
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
	# Peak RSS goes in a PARALLEL row (rss_<label>) rather than a 7th column, so
	# the existing 6-column rows stay byte-identical and the summary awk reads the
	# peak by keying on the rss_ prefix. col2 carries the peak in MB ("-" when not
	# sampled); the other columns are unused 0s. The summary degrades to "-" when
	# these rows are absent (e.g. a results.tsv from before this change).
	rss_kb=$(cat "${rss_log}" 2>/dev/null || echo "-")
	if [[ "${rss_kb}" =~ ^[0-9]+$ ]]; then
		rss_mb=$(awk -v k="${rss_kb}" 'BEGIN{printf "%.0f", k/1024}')
	else
		rss_mb="-"
	fi
	printf 'rss_%s\t%s\t0\t0\t0\t0\n' "${label}" "${rss_mb}" >>"${OUT_DIR}/results.tsv"
	printf '  %-22s tps=%-12s lat=%-9s txns=%-10s peakRSS=%sMB\n' \
		"${label}" "${tps}" "${lat}" "${txns}" "${rss_mb}"
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
	local cmd=("${SERENED_BIN}" "${datadir}" --io_threads "${IO_THREADS}")
	[[ -n "${CPU_THREADS}" ]] && cmd+=(--cpu_threads "${CPU_THREADS}")
	cmd+=("$@")
	if [[ "${SDB_DOCKER}" != 0 ]]; then
		local cpuset=() netargs=(-p "${port}:${port}")
		[[ -n "${ENG_CPUSET}" ]] && cpuset=(--cpuset-cpus "${ENG_CPUSET}")
		# serened binds the bench port itself, so host networking needs no
		# listen-port translation.
		[[ "${DOCKER_HOSTNET}" != 0 ]] && netargs=(--network host)
		docker rm -f "${SDB_CONTAINER}" >/dev/null 2>&1 || true
		docker run -d --name "${SDB_CONTAINER}" "${cpuset[@]}" \
			--user "$(id -u):$(id -g)" -e HOME=/tmp \
			"${netargs[@]}" \
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
	if [[ "${SERIALIZE}" != 0 ]]; then
		echo "  -- serialization matrix (type x text/binary, raw drain)"
		run_serialize "${srv}" "${port}"
	fi
	if [[ "${COPYIN}" != 0 ]]; then
		echo "  -- copy-in matrix (type x binary/text/csv, capture-replay)"
		run_copyin "${srv}" "${port}"
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
		# New server only: a per-cell perf.data for every serialize/copy-out and
		# copy-in drain (incl. the op/t1 variants). PROFILE_DRAIN_PID re-runs the
		# matrices in record-only mode (no extra results.tsv rows, no re-setup).
		if [[ "${srv}" == new ]]; then
			echo "  -- profiling new drains (perf.data per serialize/copy-out/copy-in cell)"
			[[ "${SERIALIZE}" != 0 ]] && PROFILE_DRAIN_PID="${CUR_PID}" run_serialize new "${port}"
			[[ "${COPYIN}" != 0 ]] && PROFILE_DRAIN_PID="${CUR_PID}" run_copyin new "${port}"
		fi
	fi
	# Connection lifecycle (-C, reconnect per transaction) runs LAST: in docker
	# mode the connect storm exhausts the host's ephemeral ports toward the
	# container (docker-proxy opens one backend connection per inbound; ~28k
	# TIME_WAIT pile up in seconds and need ~60s to expire), so any workload
	# scheduled after it gets connection resets for up to a minute.
	run_one "${srv}_connect_select1" "${port}" "${CUR_PID}" "${OUT_DIR}/wl_connect.sql" simple -C
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
	local preargs=() cargs=() netargs=()
	if [[ "${DOCKER_HOSTNET}" != 0 && -n "${ENG_PREARGS_HOST[$name]+x}" ]]; then
		read -r -a preargs <<<"${ENG_PREARGS_HOST[$name]}" || true
	else
		read -r -a preargs <<<"${ENG_PREARGS[$name]}" || true
	fi
	if [[ "${DOCKER_HOSTNET}" != 0 ]]; then
		netargs=(--network host)
		if [[ -n "${ENG_CARGS_HOST[$name]+x}" ]]; then
			read -r -a cargs <<<"${ENG_CARGS_HOST[$name]}" || true
		else
			# No listen-port knob: the engine binds its NATIVE port on the host.
			read -r -a cargs <<<"${ENG_CARGS[$name]}" || true
			hport="${cport}"
		fi
		echo "-- ${name} ${image} (host network, :${hport}) [docker${ENG_CPUSET:+, cpuset ${ENG_CPUSET}}]"
	else
		netargs=(-p "${hport}:${cport}")
		read -r -a cargs <<<"${ENG_CARGS[$name]}" || true
		echo "-- ${name} ${image} (host :${hport} -> container :${cport}) [docker${ENG_CPUSET:+, cpuset ${ENG_CPUSET}}]"
	fi
	docker rm -f "${container}" >/dev/null 2>&1 || true
	if ! docker run -d --name "${container}" "${cpuset[@]}" \
		"${netargs[@]}" "${preargs[@]}" "${image}" "${cargs[@]}" >/dev/null; then
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
	# Serialize matrix for this engine (default: all engines): real native tables
	# created the postgres way, drained as the engine's trust user/db. A cell
	# whose CREATE or drain fails (no composite type, missing function, format
	# unsupported, ...) records 0 and never hard-stops -- so a partial-capability
	# engine still yields a usable column.
	if [[ "${SERIALIZE}" != 0 ]] && in_list "${name}" "${SER_ENGINES}"; then
		echo "  -- serialize matrix (${name}; unsupported cells record 0)"
		run_serialize "${name}" "${hport}" || true
	fi
	if [[ "${COPYIN}" != 0 ]] && in_list "${name}" "${SER_ENGINES}"; then
		echo "  -- copy-in matrix (${name}; unsupported cells record 0)"
		run_copyin "${name}" "${hport}" || true
	fi
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
echo "=== benchmarking (selected engines cached/reused; new always measured) ==="
[[ -n "${DB_LIST}" ]] && echo "    engines: ${DB_LIST}"
# COMPARISON ENGINES (old serenedb release / pg / crdb / ...) -- whichever
# PERF_DB selected, all cached identically. reuse-only (set inside the partition)
# forces using the seeded cache, ignoring remeasure (a container can't schedule
# on the reserved cores).
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
bench_server new "${PORT_NEW}" "${DATA_NEW}" --listen "postgres://0.0.0.0:${PORT_NEW}"

# Summary: measured server (new) vs each comparison engine (old serenedb
# release, pg, crdb, ...) in registry order. Every engine appears in ONE column
# whose cell carries BOTH the raw number and the ratio-vs-baseline: "raw (Nx)".
# Engines absent from a category are dropped; old is just another engine.
#   1. queries   -- pgbench tps; engine cell = rawtps (new/engine); + new cyc/q
#   2. serialize + copy-out -- MB/s, split into THREE tables with identical
#      engine columns but different baselines: A=new (parallel), B=new(op)
#      (order-preserve), C=new(t1) (single thread); ----- between data types
#   3. copy-in / COPY FROM STDIN -- MB/s (single-threaded ingest); ----- between
#      data types
echo
echo "================== WIRE BENCH SUMMARY =================="
echo "clients=${CLIENTS} jobs=${JOBS} dur=${DURATION}s io_threads=${IO_THREADS}"
echo "copy-in rows=${COPYIN_ROWS}"
echo
awk -F'\t' -v engorder="${EXT_ORDER[*]}" '
{
  # Side = the engine/variant prefix; key = the workload. new(op)/new(t1) carry a
  # two-token prefix and must be split before the generic first-underscore rule,
  # else they would be misread as new_ rows and corrupt the columns.
  row=$1;
  # Peak-RSS parallel rows (rss_<side>_<workload>, col2 = peak MB or "-") are
  # stored in a side array keyed exactly like the measured row, and do NOT
  # register a workload in order/seen (no phantom rows when only RSS is present).
  is_rss=0;
  if (row ~ /^rss_/) { is_rss=1; row=substr(row,5) }
  if (row ~ /^new_op_/)      { side="new_op"; key=substr(row,8) }
  else if (row ~ /^new_t1_/) { side="new_t1"; key=substr(row,8) }
  else { p=index(row,"_"); side=substr(row,1,p-1); key=substr(row,p+1) }
  # Category by workload key: copy-in, serialize/copy-out, or plain query.
  if (key ~ /^copyin_/)                     cat="copyin";
  else if (key ~ /^ser_/ || key ~ /^copy_/) cat="serout";
  else                                      cat="query";
  if (is_rss) { peakrss[cat,key,side]=$2; next }
  tps[cat,key,side]=$2; lat[cat,key,side]=$3; txn[cat,key,side]=$4; cyc[cat,key,side]=$5;
  have[cat,side]=1;
  if (!((cat,key) in seen)) { order[cat,++n[cat]]=key; seen[cat,key]=1 }
}
# One combined cell carrying both the engine raw and the ratio-vs-baseline as
# "raw (base/raw x)". Absent/zero engine -> "-"; no baseline row -> raw only.
function cell(raw, base,   r) {
  if (raw+0<=0) return "-";
  if (base+0<=0) return sprintf("%.0f", raw);
  r=base/raw;
  return sprintf("%.0f (%.2fx)", raw, r);
}
# Peak-RSS cell for a (cat,key,side): the recorded MB, or "-" when the run did
# not sample it (docker / external engine) or these rows are absent entirely
# (a results.tsv from before peak-RSS was added). "0" is treated as missing.
function rsscell(cat, key, side,   v) {
  v=peakrss[cat,key,side];
  if (v=="" || v=="-" || v+0<=0) return "-";
  return sprintf("%d", v);
}
# One serialize/copy-out table: same engine columns and ----- group splits (on
# the 2nd _-token) across all three tables, only the ratio baseline differs.
function serout_table(basekey, basehdr, ne,   i,k,toks,t,prevtype,bn,e) {
  printf "%-24s %*s","workload",BW,basehdr;
  for (e=1;e<=ne;e++) printf " %*s",CW,ext[e];
  printf "\n";
  prevtype="";
  for (i=1;i<=n["serout"];i++) {
    k=order["serout",i]; split(k,toks,"_"); t=toks[2];
    if (prevtype!="" && t!=prevtype) print "-----";
    prevtype=t;
    bn=tps["serout",k,basekey]+0;
    printf "%-24s %*s",k,BW,(bn>0)?sprintf("%.0f",bn):"-";
    for (e=1;e<=ne;e++) printf " %*s",CW,cell(tps["serout",k,ext[e]],bn);
    printf "\n";
  }
}
END {
  nc=split(engorder,cand," ");
  CW=18;   # combined engine column width: raw + " (x.xxx)"
  BW=14;   # baseline numeric column width
  RW=13;   # peak-RSS column width (MB, "-" when not sampled)

  # ---- 1. queries ----
  print "## queries  (pgbench tps; engine cell = rawtps (new/engine); new cyc/query; new peak RSS MB)";
  ne=0; for (c=1;c<=nc;c++) if (have["query",cand[c]]) ext[++ne]=cand[c];
  printf "%-24s %*s %*s %*s","workload",BW,"new (tps)",BW,"cyc/q(new)",RW,"peakRSS(new)";
  for (e=1;e<=ne;e++) printf " %*s",CW,ext[e];
  printf "\n";
  for (i=1;i<=n["query"];i++) {
    k=order["query",i]; tn=tps["query",k,"new"]+0;
    cqn=(txn["query",k,"new"]+0>0)?sprintf("%.0f",cyc["query",k,"new"]/txn["query",k,"new"]):"n/a";
    printf "%-24s %*.0f %*s %*s",k,BW,tn,BW,cqn,RW,rsscell("query",k,"new");
    for (e=1;e<=ne;e++) printf " %*s",CW,cell(tps["query",k,ext[e]],tn);
    printf "\n";
  }

  # ---- 2. serialize + copy-out: three tables, identical engine columns, but a
  # different baseline. The parallel new runs ~all threads, so new/pg includes
  # the parallelism win; new(op)=ordered stream and new(t1)=single thread give
  # the honest per-core comparison vs (single-threaded) pg that it hides.
  show_op=have["serout","new_op"]; show_t1=have["serout","new_t1"];
  ne=0; for (c=1;c<=nc;c++) if (have["serout",cand[c]]) ext[++ne]=cand[c];
  print "";
  print "## serialize + copy-out  (MB/s; engine cell = rawMB/s (baseline/engine))";
  print "#   parallel new/pg ~= 17x is mostly parallelism (new uses all threads);";
  print "#   new(op) = ordered single stream = the per-core picture. (new(t1) =";
  print "#   forced-1-thread tracks it within noise; off by default, PERF_SER_T1=1.)";
  print "";
  print "### A. baseline new (parallel, all threads)";
  serout_table("new", "new (MB/s)", ne);
  if (show_op) {
    print ""; print "### B. baseline new(op) (order-preserving, single ordered stream)";
    serout_table("new_op", "new(op) MB/s", ne);
  }
  if (show_t1) {
    print ""; print "### C. baseline new(t1) (single thread)";
    serout_table("new_t1", "new(t1) MB/s", ne);
  }

  # ---- 3. copy-in ----
  print ""; print "## copy-in / COPY FROM STDIN  (MB/s; single-threaded ingest; cell = rawMB/s (new/engine))";
  ne=0; for (c=1;c<=nc;c++) if (have["copyin",cand[c]]) ext[++ne]=cand[c];
  printf "%-24s %*s","workload",BW,"new (MB/s)";
  for (e=1;e<=ne;e++) printf " %*s",CW,ext[e];
  printf "\n";
  prevtype="";
  for (i=1;i<=n["copyin"];i++) {
    k=order["copyin",i]; split(k,toks,"_"); t=toks[2];
    if (prevtype!="" && t!=prevtype) print "-----";
    prevtype=t;
    bn=tps["copyin",k,"new"]+0;
    printf "%-24s %*s",k,BW,(bn>0)?sprintf("%.0f",bn):"-";
    for (e=1;e<=ne;e++) printf " %*s",CW,cell(tps["copyin",k,ext[e]],bn);
    printf "\n";
  }

  # ---- 4. peak RSS (new server only) ----
  # Per-workload peak VmRSS of the bare new server, sampled across the window of
  # each run (max of a 50ms poll). Only printed when at least one workload has a
  # sampled value (docker / external-engine / pre-RSS results.tsv -> all "-",
  # the whole section is skipped). One row per workload in run order across the
  # query, serialize/copy-out and copy-in categories.
  nrss=0;
  split("query serout copyin",rcat," ");
  for (rc=1;rc<=3;rc++) {
    c=rcat[rc];
    for (i=1;i<=n[c];i++) if (rsscell(c,order[c,i],"new")!="-") nrss++;
  }
  if (nrss>0) {
    print ""; print "## peak RSS  (new server, MB; per-workload max VmRSS, bare process only)";
    printf "%-32s %*s\n","workload",RW,"peakRSS(MB)";
    for (rc=1;rc<=3;rc++) {
      c=rcat[rc];
      for (i=1;i<=n[c];i++) {
        k=order[c,i]; rv=rsscell(c,k,"new");
        if (rv!="-") printf "%-32s %*s\n",k,RW,rv;
      }
    }
  }
}' "${OUT_DIR}/results.tsv" | tee "${OUT_DIR}/summary.txt"
echo "======================================================="
echo
echo "results: ${OUT_DIR}"
if [[ "${PROFILE}" != "0" ]]; then
	echo "profiles: top_symbols_<srv>_<workload>.txt (self time), top_stacks_* (callee graph),"
	echo "          perf_*.data per query AND per new serialize/copy-out/copy-in cell"
	echo "          (incl. new_op_*/new_t1_*); open with: perf report -i <file>"
fi
