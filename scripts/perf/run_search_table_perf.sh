#!/usr/bin/env bash
# Compare three tables built by CTAS over the ClickHouse `hits` parquet dataset,
# none carrying any secondary index. The three CTAS paths exercise three write
# engines:
#   * storage='transactional' (default) -> the DuckDB store table in
#                         __sdb_store (SereneDB's catalog-managed
#                         engine_duckdb/store.db).
#   * storage='search' -> SereneDBSearchInsert (CTAS mode, iresearch
#                         columnstore via SearchTableSinkWriter)
#   * ATTACH (TYPE duckdb) -> a standalone DuckDB database, isolated from
#                         SereneDB's catalog. Same DuckDB columnar storage as the
#                         transactional engine, but with no serened code in the
#                         per-row path and in its own single file (only bind-time
#                         name resolution crosses the catalog boundary), so it's
#                         the raw-DuckDB baseline.
#
# The write is timed in two phases, reported separately:
#   * insert -- the CTAS statement itself.
#   * commit -- flushing the data to disk. search: the iresearch writer commit,
#               forced by VACUUM (REFRESH_TABLE) (no background commit thread
#               yet, and the scan can't read the rows until the segments land).
#               native: CHECKPOINT (folds the .duckdb WAL into the main file).
#               transactional: none -- the CTAS transaction is durable in the
#               store WAL at statement end; the checkpoint into store.db is
#               deferred (reported n/a; the size sums store.db + store.db.wal).
#
# Workflow:
#   1. Start serened (perf binary from build_perf/) against a fresh data dir.
#   2. With PERF_ROW_LIMIT set, sample that many rows into a standalone parquet
#      file once (off the clock); then create a view over the (sampled or full)
#      hits parquet -- never with a SQL LIMIT (see the PERF_ROW_LIMIT note).
#   3. CTAS the view into a default (transactional) table  -- insert.
#   4. CTAS the view into a storage='search' table         -- insert + commit.
#   5. ATTACH a fresh .duckdb file, CTAS the view into it   -- insert + commit.
#   6. Sanity COUNT("WatchID") over each table (one thread; see the scan note).
#   7. Report on-disk size: parquet input vs the transactional table's
#      engine_duckdb delta vs the search table's engine_search subtree (with the
#      iresearch per-extension breakdown) vs the native .duckdb file.
#
# Run this AFTER scripts/perf/download_hits.sh and after the perf build is in
# place. Output goes to scripts/perf/results/run-<timestamp>.log.
#
# Resource note: the search write path buffers the whole CTAS input in the
# per-txn LocalTableChanges ColumnDataCollection before Finalize drains it to
# iresearch. That collection is backed by DuckDB's BufferManager, so it spills
# to the temp dir under memory pressure rather than blowing the heap -- but the
# full hits.parquet (~100M rows) still materializes the entire input once (in
# memory up to the limit, then on disk). Set PERF_ROW_LIMIT to cap the row
# count on a smaller box / to bound the temp-dir spill.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"

# PARQUET_FILE     : the hits.parquet input (lives in $HOME/data by default).
# BUILD_DIR        : the perf build tree containing the release serened.
# SERENED_DATA_DIR : where serened stores its catalog/segments at runtime.
# NATIVE_DB        : the .duckdb file the serened process ATTACHes for the
#                    native-DuckDB baseline (in results/, gitignored).
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
PARQUET_DIR="${PERF_PARQUET_DIR:-${HOME}/data}"
PARQUET_FILE="${PERF_PARQUET_FILE:-${PARQUET_DIR}/hits.parquet}"
SERENED_DATA_DIR="${PERF_SERENED_DATA_DIR:-${RESULTS_DIR}/search_table_perf_data}"
NATIVE_DB="${PERF_NATIVE_DB:-${RESULTS_DIR}/search_table_perf_native.duckdb}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
PORT="${PERF_PORT:-6263}"
LOG="/tmp/${USER}-serened-search-perf.log"

# Optional row cap for the CTAS source (empty = whole file). When set, the rows
# are SAMPLED INTO A STANDALONE PARQUET FILE once during setup (off the clock)
# and the measured CTAS reads that file -- we deliberately do NOT put a SQL
# LIMIT in the view, because a LIMIT becomes a STREAMING_LIMIT operator that
# serialises the whole scan -> CTAS pipeline and would mask the parallelism of
# the transactional store / native BatchInsert writers. Also keeps the search
# write path's in-memory buffer bounded on a smaller machine.
PERF_ROW_LIMIT="${PERF_ROW_LIMIT:-}"

# CREATE TABLE AS parallelizes the source scan; the search sink Finalize runs
# single-threaded. Scan benchmarks pin to 1 because the search full-scan does
# not yet split segments across threads -- a multi-threaded transactional
# baseline would be an unfair comparison until that lands.
BUILD_THREADS="${PERF_BUILD_THREADS:-$(nproc)}"
SCAN_THREADS="${PERF_SCAN_THREADS:-1}"

# EXPLAIN each timed statement so the plan (SereneDBSearchInsert vs the
# transactional store insert, SearchFullScan vs the store table scan) sits next
# to its timing in the log. Turn off with PERF_EXPLAIN=0.
PERF_EXPLAIN="${PERF_EXPLAIN:-1}"

if [[ ! -f "${PARQUET_FILE}" ]]; then
	echo "missing ${PARQUET_FILE} -- run scripts/perf/download_hits.sh first" >&2
	echo "(override the location with PERF_PARQUET_FILE=...)" >&2
	exit 1
fi
if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN} -- build the perf binary first" >&2
	echo "(override with PERF_BUILD_DIR=...)" >&2
	exit 1
fi

mkdir -p "${RESULTS_DIR}" "${PARQUET_DIR}"
RUN_LOG="${RESULTS_DIR}/run-$(date -u +%Y%m%dT%H%M%SZ).log"

# Per-section last `Time: ...` ms value, populated by run_sql/run_setup.
declare -A TIMINGS=()

# Fresh server every run -- the CTAS time is the measurement.
killall -9 serened >/dev/null 2>&1 || true
sleep 1
rm -rf "${SERENED_DATA_DIR}"
rm -f "${NATIVE_DB}" "${NATIVE_DB}.wal"

echo "starting ${SERENED_BIN} on port ${PORT} with data dir ${SERENED_DATA_DIR}"
"${SERENED_BIN}" "${SERENED_DATA_DIR}" \
	--listen="postgres://0.0.0.0:${PORT}" \
	>"${LOG}" 2>&1 &
SERENED_PID=$!
trap "kill -9 ${SERENED_PID} >/dev/null 2>&1 || true" EXIT

# Wait until the server accepts a connection. A SELECT 1 is the cheapest PG
# reachability probe; PSQL_CONN points psql at the running instance.
PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"
for _ in $(seq 1 30); do
	if psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
		break
	fi
	sleep 0.5
done
if ! psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
	echo "serened did not come up -- last 50 lines of ${LOG}:" >&2
	tail -50 "${LOG}" >&2
	exit 1
fi

# Pull the LAST `Time: <ms> ms ...` line from the captured psql output, kept in
# fractional milliseconds (as printed by psql `\timing on`).
extract_last_time_ms() {
	local out="$1"
	awk '/^Time: /{t=$2} END{if (t!="") printf "%s", t}' <<<"${out}"
}

# threads is per-connection in DuckDB and each psql -c opens a fresh
# connection, so every invocation re-applies its own SET threads before the
# timed statement.
# Run a timed statement. A query that errors (or crashes the server) must NOT
# abort the whole run -- we still want the storage report and the summary, and
# we want the error text visible. The `if out=$(...)` form is the key: commands
# whose status is tested by `if` are exempt from `set -e`, so a non-zero psql
# exit is captured instead of killing the script. A failed query leaves its
# timing empty, which fmt_ms renders as "n/a".
run_sql() {
	local label="$1" threads="$2" sql="$3"
	printf '\n=== %s (threads=%s) ===\n' "${label}" "${threads}" |
		tee -a "${RUN_LOG}"
	if [[ "${PERF_EXPLAIN}" == "1" ]]; then
		# `|| true`: a non-explainable or failing EXPLAIN never aborts the run.
		psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
			-c '\timing off' \
			-c "SET threads = ${threads};" \
			-c "EXPLAIN ${sql}" 2>&1 | tee -a "${RUN_LOG}" || true
	fi
	local out rc=0
	if out=$(psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
		-c "SET threads = ${threads};" \
		-c '\timing on' \
		-c "${sql}" 2>&1); then
		rc=0
	else
		rc=$?
	fi
	printf '%s\n' "${out}" | tee -a "${RUN_LOG}"
	if [[ ${rc} -ne 0 ]]; then
		printf '!!! %s FAILED (psql exit %s) -- continuing to summary\n' \
			"${label}" "${rc}" | tee -a "${RUN_LOG}"
	fi
	TIMINGS["${label}"]=$(extract_last_time_ms "${out}")
}

# Same as run_sql but never runs EXPLAIN -- VACUUM is not in the grammar's
# ExplainableStmt allowlist, so EXPLAIN-ing it is a parse error.
run_setup() {
	local label="$1" threads="$2" sql="$3"
	printf '\n=== %s (threads=%s) ===\n' "${label}" "${threads}" |
		tee -a "${RUN_LOG}"
	local out rc=0
	if out=$(psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
		-c "SET threads = ${threads};" \
		-c '\timing on' \
		-c "${sql}" 2>&1); then
		rc=0
	else
		rc=$?
	fi
	printf '%s\n' "${out}" | tee -a "${RUN_LOG}"
	if [[ ${rc} -ne 0 ]]; then
		printf '!!! %s FAILED (psql exit %s) -- continuing to summary\n' \
			"${label}" "${rc}" | tee -a "${RUN_LOG}"
	fi
	TIMINGS["${label}"]=$(extract_last_time_ms "${out}")
}

du_bytes() {
	# Sum of a path's bytes, or 0 if it doesn't exist yet.
	local p="$1"
	[[ -e "${p}" ]] && du -sb "${p}" | awk '{print $1}' || echo 0
}

# --- 1. Source parquet (+ optional sample) ------------------------------------
# With no row cap we read hits.parquet directly. With PERF_ROW_LIMIT set we
# sample that many rows into a standalone parquet file ONCE (the LIMIT
# serialisation is paid here, off the benchmark clock) and point the view at
# the sample with NO limit, so every measured CTAS scan + write parallelises.
# The sample is cached next to hits.parquet (named by row count) and reused
# across runs -- delete it to regenerate.
SRC_PARQUET="${PARQUET_FILE}"
if [[ -n "${PERF_ROW_LIMIT}" ]]; then
	SRC_PARQUET="${PARQUET_DIR}/hits_sample_${PERF_ROW_LIMIT}.parquet"
	if [[ -f "${SRC_PARQUET}" ]]; then
		echo "reusing cached sample ${SRC_PARQUET}" | tee -a "${RUN_LOG}"
	else
		SAMPLE_SRC_SQL=$(printf '%s' "${PARQUET_FILE}" | sed "s/'/''/g")
		SAMPLE_OUT_SQL=$(printf '%s' "${SRC_PARQUET}" | sed "s/'/''/g")
		run_setup "sample_parquet (${PERF_ROW_LIMIT} rows, one-time)" "${BUILD_THREADS}" "
COPY (SELECT * FROM read_parquet('${SAMPLE_SRC_SQL}') LIMIT ${PERF_ROW_LIMIT})
  TO '${SAMPLE_OUT_SQL}' (FORMAT 'parquet');
"
	fi
fi

PQ_SQL_PATH=$(printf '%s' "${SRC_PARQUET}" | sed "s/'/''/g")
run_sql "create_view" "${BUILD_THREADS}" "
CREATE VIEW hits_view AS
SELECT * FROM read_parquet('${PQ_SQL_PATH}');
"

TXN_DIR="${SERENED_DATA_DIR}/engine_duckdb"
SEARCH_DIR="${SERENED_DATA_DIR}/engine_search"

# Baseline catalog/engine footprint before any user table exists. The
# transactional table's storage is the delta against this (engine_duckdb is the
# shared store.db single-file database -- it also holds catalog metadata for
# every table, negligible next to the hits data; store.db.wal under the dir is
# summed in too).
TXN_BASELINE=$(du_bytes "${TXN_DIR}")

# --- 2. CTAS: transactional table (insert only; the CTAS txn lands in the
#        store WAL, durable at statement end) ---------------------------------
run_sql "transactional_insert" "${BUILD_THREADS}" "
CREATE TABLE hits_transactional WITH (storage = 'transactional') AS
SELECT * FROM hits_view;
"
TXN_AFTER=$(du_bytes "${TXN_DIR}")
TXN_TABLE_BYTES=$((TXN_AFTER - TXN_BASELINE))

# --- 3. CTAS: search table (insert, then commit) ------------------------------
# insert: routes through SereneDBSearchInsert (CTAS mode). The rows are drained
# into the iresearch writer but the segments are not yet committed to disk.
run_sql "search_insert" "${BUILD_THREADS}" "
CREATE TABLE hits_search WITH (storage = 'search') AS
SELECT * FROM hits_view;
"

# commit: force the iresearch writer commit so the segments hit disk and the
# scan can read them (no background commit thread yet).
run_setup "search_commit" "${BUILD_THREADS}" "
VACUUM (REFRESH_TABLE) hits_search;
"
SEARCH_BYTES=$(du_bytes "${SEARCH_DIR}")

# --- 4. ATTACH + CTAS: native DuckDB table (insert, then commit) --------------
# A genuine DuckDB-native columnar table is reachable ONLY via ATTACH (TYPE
# duckdb): it spins up a separate DuckCatalog + SingleFileStorageManager in the
# same process (serened's own catalog has no kDuckDB storage kind). Queries
# against it run on DuckDB's native operators with no serened code in the
# per-row path -- the only cross-catalog cost is name resolution at bind time,
# so it's a fair engine-vs-engine baseline, not a bridged one. The table is
# fully qualified (native_db.main.*) rather than relying on search_path.
NDB_SQL_PATH=$(printf '%s' "${NATIVE_DB}" | sed "s/'/''/g")
run_setup "native_attach" "${BUILD_THREADS}" "
ATTACH '${NDB_SQL_PATH}' AS native_db (TYPE duckdb);
"

# insert: native CTAS. The bulk of the data lands in <native_db>.wal until the
# checkpoint folds it into the main file.
run_sql "native_insert" "${BUILD_THREADS}" "
CREATE TABLE native_db.main.hits_native AS
SELECT * FROM hits_view;
"

# commit: CHECKPOINT collapses native_db's WAL into the main .duckdb file --
# the analogue of the search writer commit above.
run_setup "native_commit" "${BUILD_THREADS}" "
CHECKPOINT native_db;
"
NATIVE_BYTES=$(($(du_bytes "${NATIVE_DB}") + $(du_bytes "${NATIVE_DB}.wal")))

# --- 5. Sanity scan -----------------------------------------------------------
# COUNT(<col>), pinned to one thread. It's COUNT("WatchID"), not COUNT(*),
# because COUNT(*) takes the search table's count-only fast path (answered from
# segment metadata, no column read) -- counting a real, non-null column instead
# forces an actual column scan, which is the comparison we want and also
# verifies every row is accounted for. The search side still reads far less than
# the transactional/native column stores' full scan.
run_sql "count_transactional" "${SCAN_THREADS}" "SELECT COUNT(\"WatchID\") FROM hits_transactional;"
run_sql "count_search" "${SCAN_THREADS}" "SELECT COUNT(\"WatchID\") FROM hits_search;"
run_sql "count_native" "${SCAN_THREADS}" "SELECT COUNT(\"WatchID\") FROM native_db.main.hits_native;"

# --- 6. Storage size ----------------------------------------------------------
human() {
	awk -v b="$1" 'BEGIN{
    split("B KB MB GB TB", u);
    i=1;
    while (b >= 1024 && i < 5) { b/=1024; i++ }
    printf "%.2f %s", b, u[i];
  }'
}
sum_ext() {
	local dir="$1" ext="$2"
	find "${dir}" -name "*.${ext}" -printf '%s\n' 2>/dev/null |
		awk '{s+=$1} END{printf "%d\n", s+0}'
}

pq=$(du_bytes "${SRC_PARQUET}")
total=$(du_bytes "${SERENED_DATA_DIR}")

{
	echo
	echo "=== storage_size ==="
	printf "parquet input:            %14d bytes (%s)\n" "${pq}" "$(human "${pq}")"
	printf "transactional (delta):    %14d bytes (%s)\n" \
		"${TXN_TABLE_BYTES}" "$(human "${TXN_TABLE_BYTES}")"
	printf "search table:             %14d bytes (%s)\n" \
		"${SEARCH_BYTES}" "$(human "${SEARCH_BYTES}")"
	printf "native duckdb table:      %14d bytes (%s)\n" \
		"${NATIVE_BYTES}" "$(human "${NATIVE_BYTES}")"
	printf "serened data dir (total): %14d bytes (%s)\n" "${total}" "$(human "${total}")"
	# Per-file-type breakdown for the search table (engine_search). This
	# iresearch version consolidates a segment into:
	#   .col typed columnstore (the column values -- the bulk of the data)
	#   .idx terms + postings index   .doc doc-id stream   .sm segment meta
	# .swal is the search-table WAL (engine_search/<db>/wal) -- transient, holds
	# the raw chunks until GC, so right after a load it can dwarf the committed
	# index. (segments_N commit-metadata files have no extension and are not
	# summed here; they're in the engine_search total above.)
	echo "  search table file breakdown:"
	for ext in col idx doc sm swal; do
		s=$(sum_ext "${SEARCH_DIR}" "${ext}")
		printf "    .%-5s %14d bytes (%s)\n" "${ext}" "${s}" "$(human "${s}")"
	done
} | tee -a "${RUN_LOG}"

# --- 7. Headline summary ------------------------------------------------------
fmt_ms() {
	local v="$1"
	if [[ -z "${v}" ]]; then
		printf 'n/a'
		return
	fi
	awk -v v="${v}" 'BEGIN{
    if (v + 0 >= 10000) { printf "%.2f s",  v/1000.0 }
    else if (v + 0 >= 1) { printf "%.1f ms", v + 0 }
    else                 { printf "%.3f ms", v + 0 }
  }'
}
ratio() {
	# ratio A B -> "r.rrx" if both numeric, else "n/a".
	local a="$1" b="$2"
	if [[ -z "${a}" || -z "${b}" ]]; then
		printf 'n/a'
		return
	fi
	awk -v a="${a}" -v b="${b}" 'BEGIN{
    if (b + 0 == 0) { printf "n/a"; exit }
    r = a / b;
    if (r >= 100) printf "%.0fx", r;
    else if (r >= 10) printf "%.1fx", r;
    else printf "%.2fx", r;
  }'
}
{
	echo
	echo "================ SUMMARY ================"
	echo "parquet:   ${SRC_PARQUET}"
	echo "row limit: ${PERF_ROW_LIMIT:-(none -- full file)}"
	echo "build threads: ${BUILD_THREADS}    scan threads: ${SCAN_THREADS}"
	echo
	# insert / commit reported separately. transactional has no separate commit
	# phase here (the CTAS txn is durable in the store WAL; checkpoint deferred).
	# search commit = the iresearch writer flush; native commit = CHECKPOINT
	# (WAL -> main .duckdb file). The ratio columns are each engine's
	# insert+commit total relative to transactional (>1x slower, <1x faster).
	t_ins="${TIMINGS[transactional_insert]:-}"
	s_ins="${TIMINGS[search_insert]:-}"
	s_cmt="${TIMINGS[search_commit]:-}"
	n_ins="${TIMINGS[native_insert]:-}"
	n_cmt="${TIMINGS[native_commit]:-}"
	sum2() { # "a+b" in ms, or empty if either is empty
		[[ -n "$1" && -n "$2" ]] && awk -v a="$1" -v b="$2" 'BEGIN{printf "%s", a+b}'
	}
	s_total=$(sum2 "${s_ins}" "${s_cmt}")
	n_total=$(sum2 "${n_ins}" "${n_cmt}")
	c_t="${TIMINGS[count_transactional]:-}"
	c_s="${TIMINGS[count_search]:-}"
	c_n="${TIMINGS[count_native]:-}"
	row() { printf "%-16s %11s %11s %11s   %7s %7s\n" "$1" "$2" "$3" "$4" "$5" "$6"; }
	row "phase" "txn" "search" "native" "s/txn" "n/txn"
	row "----------------" "-----------" "-----------" "-----------" "-------" "-------"
	row "insert" "$(fmt_ms "${t_ins}")" "$(fmt_ms "${s_ins}")" "$(fmt_ms "${n_ins}")" "" ""
	row "commit" "n/a" "$(fmt_ms "${s_cmt}")" "$(fmt_ms "${n_cmt}")" "" ""
	row "total (ins+cmt)" "$(fmt_ms "${t_ins}")" "$(fmt_ms "${s_total}")" "$(fmt_ms "${n_total}")" \
		"$(ratio "${s_total}" "${t_ins}")" "$(ratio "${n_total}" "${t_ins}")"
	row "count(col)" "$(fmt_ms "${c_t}")" "$(fmt_ms "${c_s}")" "$(fmt_ms "${c_n}")" \
		"$(ratio "${c_s}" "${c_t}")" "$(ratio "${c_n}" "${c_t}")"
	echo
	printf "%-26s %14s   %s\n" "storage" "bytes" "vs txn"
	printf "%-26s %14s   %s\n" "--------------------------" "--------------" "----------"
	printf "%-26s %14d (%s)\n" "parquet input" "${pq}" "$(human "${pq}")"
	printf "%-26s %14d (%s)\n" "transactional table" "${TXN_TABLE_BYTES}" "$(human "${TXN_TABLE_BYTES}")"
	printf "%-26s %14d (%s)   %s\n" "search table" "${SEARCH_BYTES}" "$(human "${SEARCH_BYTES}")" \
		"$(ratio "${SEARCH_BYTES}" "${TXN_TABLE_BYTES}")"
	printf "%-26s %14d (%s)   %s\n" "native duckdb table" "${NATIVE_BYTES}" "$(human "${NATIVE_BYTES}")" \
		"$(ratio "${NATIVE_BYTES}" "${TXN_TABLE_BYTES}")"
	echo "========================================="
} | tee -a "${RUN_LOG}"

echo
echo "log: ${RUN_LOG}"
echo "server log: ${LOG}"
