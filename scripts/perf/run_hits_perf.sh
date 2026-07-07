#!/usr/bin/env bash
# Compare iresearch-columnstore (PG INCLUDE) vs raw parquet on the ClickHouse
# `hits` dataset.
#
# Workflow:
#   1. Start serened (perf binary from build_perf/) against a fresh
#      build_perf_data/ catalog/segment dir.
#   2. ATTACH a fresh DuckDB-native catalog (the third data point); set
#      search_path so its schema is visible alongside serened's default.
#   3. Create a view over read_parquet on the hits dataset (default in
#      $HOME/data/hits.parquet).
#   4. CTAS that view into a native DuckDB table (analytics gold standard).
#   5. Build an inverted index on a tokenized column with all the analytics
#      columns in INCLUDE -- that exercises iresearch_scan + the columnstore
#      materializer.
#   6. Run the benchmark suite three times per query, timing against:
#        a) the indexed view  -> iresearch columnstore path
#        b) the bare parquet read -> DuckDB read_parquet path
#        c) the native table  -> DuckDB native columnar storage
#   7. Report on-disk size: parquet vs build_perf_data vs the .duckdb file,
#      plus per-extension iresearch breakdown.
#
# Run this AFTER `scripts/perf/download_hits.sh` and after the perf build is
# in place. Output goes to scripts/perf/results/<timestamp>.log.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"

# Three separate paths -- do not conflate:
#   PARQUET_FILE      : the hits.parquet input (lives in $HOME/data by default).
#   BUILD_DIR         : the perf build tree containing the release serened.
#   SERENED_DATA_DIR  : where serened stores its catalog/segments at runtime.
#   NATIVE_DB         : .duckdb file the perf serened ATTACHes; in
#                       scripts/perf/results/ (gitignored).
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
PARQUET_DIR="${PERF_PARQUET_DIR:-${HOME}/data}"
PARQUET_FILE="${PERF_PARQUET_FILE:-${PARQUET_DIR}/hits.parquet}"
SERENED_DATA_DIR="${PERF_SERENED_DATA_DIR:-${RESULTS_DIR}/hits_perf_data}"
NATIVE_DB="${PERF_NATIVE_DB:-${RESULTS_DIR}/hits_perf_native.duckdb}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
PORT="${PERF_PORT:-6262}"
LOG="/tmp/${USER}-serened-perf.log"

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

# Per-section last `Time: ...` ms value, populated by run_sql/run_setup. Used
# by the summary block at the end of the script for a single shareable table.
declare -A TIMINGS=()

# Fresh server every run -- the index build time is part of the measurement.
killall -9 serened >/dev/null 2>&1 || true
sleep 1
rm -rf "${SERENED_DATA_DIR}"
rm -f "${NATIVE_DB}" "${NATIVE_DB}.wal"

echo "starting ${SERENED_BIN} on port ${PORT} with data dir ${SERENED_DATA_DIR}"
"${SERENED_BIN}" "${SERENED_DATA_DIR}" \
	--listen "postgres://0.0.0.0:${PORT}" \
	>"${LOG}" 2>&1 &
SERENED_PID=$!
trap "kill -9 ${SERENED_PID} >/dev/null 2>&1 || true" EXIT

# Wait until the server accepts a connection. PG protocol -- a SELECT 1 is the
# cheapest reachability probe; PSQL_CONN points psql at the running instance.
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

# SET threads is a per-connection setting in DuckDB, and each psql call
# below opens a fresh connection -- so each invocation must apply its own
# value before the timed statement runs.
#
# Index build parallelizes today (iresearch SegmentWriter + commit pool), so
# CREATE INDEX uses nproc. Scan benchmarks pin to 1 because iresearch's
# full-scan does not yet split segments across threads -- multi-threaded
# DuckDB on the parquet baseline would be an unfair comparison until that
# lands.
BUILD_THREADS="${PERF_BUILD_THREADS:-$(nproc)}"
SCAN_THREADS="${PERF_SCAN_THREADS:-1}"

# EXPLAIN is supported for CREATE INDEX too -- having the plan in the log
# next to the timing makes it easy to see which operator each phase routes
# through (iresearch_scan vs read_parquet, SearchFullScan vs IndexSource,
# the CREATE INDEX sink pipeline, etc). Guarded by PERF_EXPLAIN so a noisy
# log can be turned off (PERF_EXPLAIN=0). The EXPLAIN psql also forces
# `\timing off` so a user's ~/.psqlrc that enables it globally doesn't bleed
# fake timings onto the plan output.
PERF_EXPLAIN="${PERF_EXPLAIN:-1}"

# search_path is per-connection in DuckDB. Each psql -c spawns a fresh
# session, so the SET search_path issued during attach_native_db doesn't
# carry over -- subsequent helpers must re-apply it. ATTACH itself is
# server-side and survives across sessions.
SEARCH_PATH_SQL="SET search_path TO public, native_db.main;"

# Pull the LAST `Time: <ms> ms ...` line from the captured psql output. We
# keep it in fractional milliseconds (as printed by psql `\timing on`) so the
# summary table reproduces what the user already sees inline.
extract_last_time_ms() {
	local out="$1"
	awk '/^Time: /{t=$2} END{if (t!="") printf "%s", t}' <<<"${out}"
}

run_sql() {
	local label="$1" threads="$2" sql="$3"
	printf '\n=== %s (threads=%s) ===\n' "${label}" "${threads}" |
		tee -a "${RUN_LOG}"
	if [[ "${PERF_EXPLAIN}" == "1" ]]; then
		psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
			-c '\timing off' \
			-c "SET threads = ${threads};" \
			-c "${SEARCH_PATH_SQL}" \
			-c "EXPLAIN ${sql}" 2>&1 | tee -a "${RUN_LOG}"
	fi
	local out
	out=$(psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
		-c "SET threads = ${threads};" \
		-c "${SEARCH_PATH_SQL}" \
		-c '\timing on' \
		-c "${sql}" 2>&1)
	printf '%s\n' "${out}" | tee -a "${RUN_LOG}"
	TIMINGS["${label}"]=$(extract_last_time_ms "${out}")
}

# Same as run_sql but never runs EXPLAIN -- the grammar's ExplainableStmt
# allowlist excludes ATTACH/SET/etc., and EXPLAIN-ing them is a parse error.
# Skips the SEARCH_PATH_SQL prefix because attach_native_db itself is the
# call that establishes search_path; before ATTACH runs there's no
# `native_db` schema to reference and SET would error.
run_setup() {
	local label="$1" threads="$2" sql="$3"
	printf '\n=== %s (threads=%s) ===\n' "${label}" "${threads}" |
		tee -a "${RUN_LOG}"
	local out
	out=$(psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
		-c "SET threads = ${threads};" \
		-c '\timing on' \
		-c "${sql}" 2>&1)
	printf '%s\n' "${out}" | tee -a "${RUN_LOG}"
	TIMINGS["${label}"]=$(extract_last_time_ms "${out}")
}

# --- 1. Schema + view + index build -------------------------------------------
PQ_SQL_PATH=$(printf '%s' "${PARQUET_FILE}" | sed "s/'/''/g")
NDB_SQL_PATH=$(printf '%s' "${NATIVE_DB}" | sed "s/'/''/g")

# Attach a fresh DuckDB-native catalog so the same serened session can host
# both the iresearch index (in serened's default schema) and a native DuckDB
# table (in `native_db.main`). search_path puts `public` first so unqualified
# CREATE/INSERT keeps landing in serened's catalog; native_db is second so
# bare references like `hits_native` still resolve. Native CREATE TABLE is
# explicitly qualified (matches the catalog_iceberg.test_slow convention).
run_setup "attach_native_db" "${BUILD_THREADS}" "
ATTACH '${NDB_SQL_PATH}' AS native_db (TYPE duckdb, STORAGE_VERSION latest);
SET search_path TO public, native_db.main;
"

run_sql "create_view" "${BUILD_THREADS}" "
CREATE VIEW hits_view AS
SELECT * FROM read_parquet('${PQ_SQL_PATH}');
"

# CTAS into the attached native catalog -- the gold-standard analytics
# baseline (full DuckDB columnar storage with codec selection, zone maps,
# etc.). Comparable in role to CREATE INDEX on the serened side, so we time
# it under the same BUILD_THREADS thread budget.
run_sql "create_native_table" "${BUILD_THREADS}" "
CREATE TABLE native_db.main.hits_native AS
SELECT * FROM hits_view;
"

# Snapshot post-CTAS sizes BEFORE checkpoint. After CTAS the bulk of the
# data lives in <native_db>.wal; CHECKPOINT then folds the wal into the main
# file. Reporting both lets us see the actual end-state vs the
# WAL-as-pending-write state.
NATIVE_SIZE_PRE_MAIN=0
NATIVE_SIZE_PRE_WAL=0
[[ -f "${NATIVE_DB}" ]] && NATIVE_SIZE_PRE_MAIN=$(du -sb "${NATIVE_DB}" | awk '{print $1}')
[[ -f "${NATIVE_DB}.wal" ]] && NATIVE_SIZE_PRE_WAL=$(du -sb "${NATIVE_DB}.wal" | awk '{print $1}')

# CHECKPOINT collapses native_db's WAL into the main file before we measure
# its final on-disk size.
run_setup "checkpoint_native_db" "${BUILD_THREADS}" "
CHECKPOINT native_db;
"

# `Title` is a free-text-ish column (page title) -- a natural fit for the
# tokenized inverted index. Everything else useful for analytics goes into
# INCLUDE so it lands in the typed columnstore.
#
# Dictionary and index live in separate psql -c invocations: when both run in
# the same Simple Query batch, the CREATE INDEX binder doesn't see the freshly
# committed dictionary entry and fails with "does not exist".
#
# Dictionary template:
#   PERF_DICT_TEMPLATE=delimiter (default)  - whitespace split, cheapest;
#                                             removes tokenizer variability
#                                             when comparing to parquet/native.
#   PERF_DICT_TEMPLATE=text                 - full ICU `text` pipeline with
#                                             en_US.UTF-8 locale, stemming and
#                                             stopwords off, position+frequency
#                                             on. Use to measure the realistic
#                                             text-indexing cost.
PERF_DICT_TEMPLATE="${PERF_DICT_TEMPLATE:-delimiter}"
case "${PERF_DICT_TEMPLATE}" in
delimiter)
	DICT_SQL="CREATE TEXT SEARCH DICTIONARY perf_english(
    template = 'delimiter',
    delimiter = ' '
);"
	;;
text)
	DICT_SQL="CREATE TEXT SEARCH DICTIONARY perf_english(
    template = 'text',
    locale = 'en_US.UTF-8',
    case = 'none',
    stemming = false,
    accent = false,
    frequency = true,
    position = true
);"
	;;
*)
	echo "PERF_DICT_TEMPLATE must be 'delimiter' or 'text', got '${PERF_DICT_TEMPLATE}'" >&2
	exit 1
	;;
esac
run_sql "create_text_search_dict (${PERF_DICT_TEMPLATE})" "${BUILD_THREADS}" "${DICT_SQL}"

# Every column (including Title itself) goes into INCLUDE so the .cs storage
# and the create_index timing are directly comparable to native_db.hits_native
# (which always materializes every column). Derive the list from `DESCRIBE`:
# both information_schema.columns and duckdb_columns() are empty for a
# read_parquet-backed view in serened today, but DESCRIBE enumerates them.
# quote each name so quoted CamelCase identifiers match the case-insensitive
# column resolver.
INCLUDE_LIST=$(psql "${PSQL_CONN}" -At -v ON_ERROR_STOP=1 -X -c "
SELECT string_agg('\"' || column_name || '\"', ', ')
FROM (DESCRIBE hits_view);
")
if [[ -z "${INCLUDE_LIST}" ]]; then
	echo "failed to derive INCLUDE column list from hits_view" >&2
	exit 1
fi

run_sql "create_index" "${BUILD_THREADS}" "
CREATE INDEX hits_idx ON hits_view USING inverted(\"Title\" perf_english)
INCLUDE (${INCLUDE_LIST});"

# --- 2. Benchmarks ------------------------------------------------------------
# Three-way: (indexed iresearch path, parquet read, native DuckDB table).
# Each query repeats with the same shape but a different source object.

run_sql "bench_count_indexed" "${SCAN_THREADS}" "SELECT COUNT(*) FROM hits_idx;"
run_sql "bench_count_parquet" "${SCAN_THREADS}" "SELECT COUNT(*) FROM hits_view;"
run_sql "bench_count_native" "${SCAN_THREADS}" "SELECT COUNT(*) FROM hits_native;"

run_sql "bench_count_distinct_indexed" "${SCAN_THREADS}" "SELECT COUNT(DISTINCT \"UserID\") FROM hits_idx;"
run_sql "bench_count_distinct_parquet" "${SCAN_THREADS}" "SELECT COUNT(DISTINCT \"UserID\") FROM hits_view;"
run_sql "bench_count_distinct_native" "${SCAN_THREADS}" "SELECT COUNT(DISTINCT \"UserID\") FROM hits_native;"

# `has_any_tokens` works with any tokenizer (no positions/frequency needed),
# so it pairs with the `delimiter` template above. The parquet/native
# baselines use a substring filter as the closest analogue.
run_sql "bench_filter_indexed" "${SCAN_THREADS}" "SELECT COUNT(*) FROM hits_idx WHERE has_any_tokens(\"Title\", 'news');"
run_sql "bench_filter_parquet" "${SCAN_THREADS}" "SELECT COUNT(*) FROM hits_view WHERE \"Title\" ILIKE '%news%';"
run_sql "bench_filter_native" "${SCAN_THREADS}" "SELECT COUNT(*) FROM hits_native WHERE \"Title\" ILIKE '%news%';"

run_sql "bench_groupby_indexed" "${SCAN_THREADS}" "
SELECT \"RegionID\", COUNT(*) AS hits FROM hits_idx
GROUP BY \"RegionID\" ORDER BY hits DESC LIMIT 10;"
run_sql "bench_groupby_parquet" "${SCAN_THREADS}" "
SELECT \"RegionID\", COUNT(*) AS hits FROM hits_view
GROUP BY \"RegionID\" ORDER BY hits DESC LIMIT 10;"
run_sql "bench_groupby_native" "${SCAN_THREADS}" "
SELECT \"RegionID\", COUNT(*) AS hits FROM hits_native
GROUP BY \"RegionID\" ORDER BY hits DESC LIMIT 10;"

run_sql "bench_topk_indexed" "${SCAN_THREADS}" "
SELECT \"UserID\", COUNT(*) AS hits FROM hits_idx
GROUP BY \"UserID\" ORDER BY hits DESC LIMIT 10;"
run_sql "bench_topk_parquet" "${SCAN_THREADS}" "
SELECT \"UserID\", COUNT(*) AS hits FROM hits_view
GROUP BY \"UserID\" ORDER BY hits DESC LIMIT 10;"
run_sql "bench_topk_native" "${SCAN_THREADS}" "
SELECT \"UserID\", COUNT(*) AS hits FROM hits_native
GROUP BY \"UserID\" ORDER BY hits DESC LIMIT 10;"

# --- 3. Storage size ----------------------------------------------------------
# The print block below partitions the data dir into iresearch segment files
# (per extension), the iresearch `segments_N` per-commit manifests, the
# RocksDB engine state under engine_rocksdb/, and the top-level serened
# catalog files (LANGUAGE, LOCK, VERSION-*, SERVER). The `other` line should
# always read 0 -- if it doesn't, a new file kind has appeared and the
# inventory comment in the print block needs updating.
sum_ext() {
	local dir="$1" ext="$2"
	find "${dir}" -name "*.${ext}" -printf '%s\n' 2>/dev/null |
		awk '{s+=$1} END{printf "%d\n", s+0}'
}
human() {
	awk -v b="$1" 'BEGIN{
    split("B KB MB GB TB", u);
    i=1;
    while (b >= 1024 && i < 5) { b/=1024; i++ }
    printf "%.2f %s", b, u[i];
  }'
}
# Compute sizes in the parent shell so the SUMMARY block below can re-use
# the same numbers without a second `du` round-trip and without inheriting
# subshell scope from the storage_size print.
pq=$(du -sb "${PARQUET_FILE}" | awk '{print $1}')
total=$(du -sb "${SERENED_DATA_DIR}" | awk '{print $1}')
ndb=0
ndb_wal=0
if [[ -f "${NATIVE_DB}" ]]; then
	ndb=$(du -sb "${NATIVE_DB}" | awk '{print $1}')
fi
if [[ -f "${NATIVE_DB}.wal" ]]; then
	ndb_wal=$(du -sb "${NATIVE_DB}.wal" | awk '{print $1}')
fi
ndb_post_total=$((ndb + ndb_wal))
ndb_pre_total=$((NATIVE_SIZE_PRE_MAIN + NATIVE_SIZE_PRE_WAL))

{
	echo
	echo "=== storage_size ==="
	printf "parquet:                  %12d bytes (%s)\n" "${pq}" "$(human "${pq}")"
	printf "duckdb native pre-ckpt:   %12d bytes (%s) [main=%s wal=%s]\n" \
		"${ndb_pre_total}" "$(human "${ndb_pre_total}")" \
		"$(human "${NATIVE_SIZE_PRE_MAIN}")" "$(human "${NATIVE_SIZE_PRE_WAL}")"
	printf "duckdb native post-ckpt:  %12d bytes (%s) [main=%s wal=%s]\n" \
		"${ndb_post_total}" "$(human "${ndb_post_total}")" \
		"$(human "${ndb}")" "$(human "${ndb_wal}")"
	printf "serened data dir:         %12d bytes (%s)\n" "${total}" "$(human "${total}")"
	# Every file in the data dir falls into exactly one of these buckets:
	#   iresearch per-segment files (in engine_search/.../<seg>.<ext>):
	#     .doc  postings doc-id stream
	#     .pos  positions stream      (0 bytes with delimiter dict; populated with text)
	#     .pay  payload stream        (0 bytes without frequency/payload features)
	#     .tm   terms data            (FST-encoded term suffixes)
	#     .ti   terms index           (top-level pointers into .tm)
	#     .sm   segment_meta
	#     .cs   new typed columnstore (this work)
	#     .csd  legacy columnstore data
	#     .csi  legacy columnstore index
	#   iresearch directory-level files:
	#     segments_N  per-commit manifest
	#   RocksDB engine (engine_rocksdb/*: LOCK, LOG, MANIFEST-*, OPTIONS-*,
	#                   IDENTITY, CURRENT, journals/*.log).
	#   serened catalog (top-level files: LANGUAGE, LOCK, VERSION-*, SERVER).
	# `other` should always print 0 -- if it doesn't, the data dir grew a
	# new file kind we haven't accounted for yet.
	sum_glob() {
		find "$@" -printf '%s\n' 2>/dev/null |
			awk '{s+=$1} END{printf "%d\n", s+0}'
	}
	declare -a exts=(doc pos pay cs csd csi ti tm sm)
	ext_total=0
	for ext in "${exts[@]}"; do
		s=$(sum_ext "${SERENED_DATA_DIR}" "${ext}")
		ext_total=$((ext_total + s))
		printf "  .%-7s %14d bytes (%s)\n" "${ext}" "${s}" "$(human "${s}")"
	done
	# iresearch per-commit manifest: `segments_N` (no extension).
	seg_total=$(sum_glob "${SERENED_DATA_DIR}" -name 'segments_*' -type f)
	printf "  %-8s %14d bytes (%s)\n" "segments_*" "${seg_total}" "$(human "${seg_total}")"
	# RocksDB engine state.
	rdb_total=$(sum_glob "${SERENED_DATA_DIR}/engine_rocksdb" -type f)
	printf "  %-8s %14d bytes (%s)\n" "rocksdb" "${rdb_total}" "$(human "${rdb_total}")"
	# Top-level serened catalog files.
	cat_total=$(sum_glob "${SERENED_DATA_DIR}" -maxdepth 1 -type f)
	printf "  %-8s %14d bytes (%s)\n" "catalog" "${cat_total}" "$(human "${cat_total}")"
	classified=$((ext_total + seg_total + rdb_total + cat_total))
	other=$((total - classified))
	printf "  %-8s %14d bytes (%s)\n" "other" "${other}" "$(human "${other}")"
} | tee -a "${RUN_LOG}"

# --- 4. Headline summary ------------------------------------------------------
# Single shareable table at the bottom: build phase, three-way query timings,
# storage. Designed to be the one thing a colleague needs to read.
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
	# ratio A B -> "AxxBx" if both numeric, else "n/a"
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
	echo "parquet:   ${PARQUET_FILE}"
	echo "build threads: ${BUILD_THREADS}    scan threads: ${SCAN_THREADS}"
	echo "Title dictionary template: ${PERF_DICT_TEMPLATE}"
	echo
	printf "%-20s %12s\n" "phase" "time"
	printf "%-20s %12s\n" "--------------------" "------------"
	printf "%-20s %12s\n" "create_native_table" "$(fmt_ms "${TIMINGS[create_native_table]:-}")"
	printf "%-20s %12s\n" "checkpoint_native" "$(fmt_ms "${TIMINGS[checkpoint_native_db]:-}")"
	printf "%-20s %12s\n" "create_index" "$(fmt_ms "${TIMINGS[create_index]:-}")"
	echo
	printf "%-22s %12s %12s %12s   %s\n" "query" "indexed" "parquet" "native" "speedup vs (parquet | native)"
	printf "%-22s %12s %12s %12s   %s\n" "----------------------" "------------" "------------" "------------" "-----------------------------"
	# Each row: query name, three timings, ratio vs parquet, ratio vs native.
	for q in count count_distinct filter groupby topk; do
		i="${TIMINGS[bench_${q}_indexed]:-}"
		p="${TIMINGS[bench_${q}_parquet]:-}"
		n="${TIMINGS[bench_${q}_native]:-}"
		printf "%-22s %12s %12s %12s   %-8s | %-8s\n" \
			"${q}" "$(fmt_ms "${i}")" "$(fmt_ms "${p}")" "$(fmt_ms "${n}")" \
			"$(ratio "${p}" "${i}")" "$(ratio "${n}" "${i}")"
	done
	echo
	printf "%-26s %12s\n" "storage" "bytes"
	printf "%-26s %12s\n" "--------------------------" "------------"
	printf "%-26s %12s (%s)\n" "parquet" "${pq}" "$(human "${pq}")"
	printf "%-26s %12s (%s)\n" "duckdb native (post-ckpt)" "${ndb_post_total}" "$(human "${ndb_post_total}")"
	printf "%-26s %12s (%s)\n" "serened data dir" "${total}" "$(human "${total}")"
	echo "========================================="
} | tee -a "${RUN_LOG}"

echo
echo "log: ${RUN_LOG}"
echo "server log: ${LOG}"
