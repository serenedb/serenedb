#!/usr/bin/env bash
# bench_search_table_pk.sh -- what a declared PRIMARY KEY costs a search table
# on the write path.
#
# The comparison this script is built for is ACROSS RUNS, not within one: the
# same schema and the same statements, run against a binary that enforces the
# key and against one where it is declared and inert (main), with
# PERF_SERENED_BIN pointed at each in turn. Schema and term indexing are
# identical in both -- PrimaryKeyConfig term-indexes the key columns on main
# already -- so the difference is certification and nothing else.
#
# Two shapes, because they exercise different halves:
#   * one big transaction -- the statement-end probe (one per statement) and
#     the in-transaction key index. The commit passes run ONCE for the whole
#     load and are invisible here.
#   * many small transactions -- per-commit cost: the pre-lock pass and the
#     pass under the commit lock, each a read of the WAL tail, once per
#     transaction.
#
# Why not CTAS, which run_search_table_perf.sh uses: CREATE TABLE AS cannot
# carry a constraint, and ALTER TABLE ADD PRIMARY KEY is refused on a
# search-backed table, so a PK'd table can only be loaded by INSERT ... SELECT.
# The source is staged into a transactional table off the clock so each batch
# is a cheap range scan rather than a re-scan of the parquet.
#
# The key column is "WatchID": unique in the 1M and 10M samples (checked), four
# duplicates in the full 100M file, so the full file needs deduping first --
# PERF_PK_DEDUP=1 does it off the clock. It is also a random-ish u64, which is
# the realistic term-dictionary spread; a synthetic row_number() key would be
# monotonic and would flatter the probe.
#
# Pre-reqs: a serened binary (any build -- use the perf preset for numbers, a
# debug build only to check the script runs), and the hits parquet from
# scripts/perf/download_hits.sh.
#
# It runs outside a checkout too, with the binary and the parquet wherever they
# are: PERF_SERENED_BIN, PERF_PARQUET_FILE and PERF_RESULTS_DIR between them
# leave nothing resolved against the repo.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"

usage() {
	cat <<'EOF'
usage: bench_search_table_pk.sh

Takes no options; everything is a PERF_* environment variable, listed with
its default below.
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		usage >&2
		exit 2
		;;
	esac
done

BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
SERENED_BIN="${PERF_SERENED_BIN:-${BUILD_DIR}/bin/serened}"
RESULTS_DIR="${PERF_RESULTS_DIR:-${ROOT}/scripts/perf/results}"
PARQUET_DIR="${PERF_PARQUET_DIR:-${HOME}/data}"
PARQUET_FILE="${PERF_PARQUET_FILE:-${PARQUET_DIR}/hits.parquet}"
DATA_DIR="${PERF_PK_DATA_DIR:-${RESULTS_DIR}/search_pk_perf_data}"
PORT="${PERF_PORT:-6264}"
LOG="/tmp/${USER:-$(id -un)}-serened-search-pk-perf.log"

# Rows to load. Empty = the whole parquet file.
PERF_PK_ROWS="${PERF_PK_ROWS:-}"
# Batches for the many-small-transactions shape. Each is one autocommit
# INSERT ... SELECT over a row-number range of the staged source.
PERF_PK_BATCHES="${PERF_PK_BATCHES:-100}"
# Dedupe the source on the key before staging. Needed for the full 100M file
# (4 duplicate WatchIDs); the 1M and 10M samples are already unique.
PERF_PK_DEDUP="${PERF_PK_DEDUP:-0}"
# WITH options for the loaded tables. refresh_interval=0 holds the
# committed-but-unpublished window open, so every commit pass reads a tail that
# grows for the whole load instead of one a publish keeps trimming.
PERF_PK_REFRESH_INTERVAL="${PERF_PK_REFRESH_INTERVAL:-}"
PERF_PK_COMPACTION_INTERVAL="${PERF_PK_COMPACTION_INTERVAL:-0}"
# Columns of the primary key. Any list starting with watchid stays unique,
# since watchid alone is -- which is what the staging check below verifies, so
# a wider key needs no separate guarantee. Widening it is how the keys a
# transaction carries are made expensive: an encoded BIGINT fits in a term's
# inline buffer, while a text column does not, so each key a commit holds in
# memory becomes a heap allocation instead of a free one.
PERF_PK_COLUMNS="${PERF_PK_COLUMNS:-watchid}"
# Also load a table with NO primary key, for within-binary context. Labelled
# separately in the summary because it does NOT isolate certification: without
# a PK the key column is not term-indexed either, so the gap includes indexing.
PERF_PK_CONTROL="${PERF_PK_CONTROL:-1}"
# Also load the same rows into an ordinary transactional table, with the key
# and without it. That pair is the yardstick the search-table pair has none of
# on its own: it says what enforcing a primary key costs an engine that does it
# with an ART index over local storage, against a search table that certifies
# against published segments and an unpublished tier. Same columns, same
# statements, same shapes, so only the storage differs.
PERF_PK_REFERENCE="${PERF_PK_REFERENCE:-1}"
PERF_PK_THREADS="${PERF_PK_THREADS:-$(nproc)}"
PERF_EXPLAIN="${PERF_EXPLAIN:-0}"

if [[ ! -f "${PARQUET_FILE}" ]]; then
	echo "missing ${PARQUET_FILE} -- run scripts/perf/download_hits.sh first" >&2
	echo "(override with PERF_PARQUET_FILE=...)" >&2
	exit 1
fi
if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN} -- build it first" >&2
	echo "(override with PERF_SERENED_BIN=... or PERF_BUILD_DIR=...)" >&2
	exit 1
fi

mkdir -p "${RESULTS_DIR}"
RUN_LOG="${RESULTS_DIR}/pk-$(date -u +%Y%m%dT%H%M%SZ).log"
PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"

declare -A TIMINGS=()
declare -A PEAK_RSS=()
declare -A HELD_RSS=()
SERVER_PID=""

# The columns loaded alongside the key. Kept narrow on purpose: a wide row
# would bury the key's cost under column encoding. All are hits columns; the
# server runs with preserve_identifier_case = false, so the parquet's mixed
# case is quoted on the way in and lower case thereafter.
# EventTime arrives as an int64 epoch in the parquet, not a TIMESTAMP, so it is
# carried as BIGINT -- a cast here would be measured work that has nothing to
# do with the key.
# Title is substituted where it is null or empty, because a search table takes
# an empty term in a key column for a null and refuses it. A placeholder keeps
# every row and changes no key's uniqueness -- watchid alone already decides
# that -- and is staged off the clock, so a run with title in the key measures
# the same load as one without it.
SRC_SELECT="\"WatchID\" AS watchid, \"URL\" AS url, coalesce(nullif(\"Title\", ''), 'none') AS title, \"EventTime\" AS eventtime"
COL_DEFS='watchid BIGINT, url TEXT, title TEXT, eventtime BIGINT'
COL_LIST='watchid, url, title, eventtime'

table_opts() {
	local opts="storage='search'"
	[[ -n "${PERF_PK_REFRESH_INTERVAL}" ]] &&
		opts="${opts}, refresh_interval = ${PERF_PK_REFRESH_INTERVAL}"
	[[ -n "${PERF_PK_COMPACTION_INTERVAL}" ]] &&
		opts="${opts}, compaction_interval = ${PERF_PK_COMPACTION_INTERVAL}"
	printf '%s' "${opts}"
}

cleanup() {
	if command -v pkill >/dev/null 2>&1; then
		pkill -9 -f -- "${DATA_DIR}" >/dev/null 2>&1 || true
	else
		killall -9 serened >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

start_serened() {
	local freed=""
	for _ in $(seq 1 60); do
		cleanup
		if ! psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
			freed=1
			break
		fi
		sleep 0.5
	done
	if [[ -z "${freed}" ]]; then
		echo "port ${PORT} still serving -- free it or set PERF_PORT" >&2
		exit 1
	fi
	rm -rf "${DATA_DIR}"
	echo "starting ${SERENED_BIN} on port ${PORT}" | tee -a "${RUN_LOG}"
	"${SERENED_BIN}" "${DATA_DIR}" --listen="postgres://127.0.0.1:${PORT}" \
		>"${LOG}" 2>&1 &
	SERVER_PID=$!
	disown 2>/dev/null || true
	for _ in $(seq 1 60); do
		psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1 && return
		sleep 0.5
	done
	echo "serened did not come up -- last 50 lines of ${LOG}:" >&2
	tail -50 "${LOG}" >&2
	exit 1
}

extract_last_time_ms() {
	awk '/^Time: /{t=$2} END{if (t!="") printf "%s", t}' <<<"$1"
}

# A timed statement. Tested by `if`, so `set -e` lets a failure through to the
# summary with its error text instead of killing the run.
run_sql() {
	local label="$1" sql="$2"
	printf '\n=== %s ===\n' "${label}" | tee -a "${RUN_LOG}"
	if [[ "${PERF_EXPLAIN}" == "1" ]]; then
		psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
			-c "SET threads = ${PERF_PK_THREADS};" \
			-c "EXPLAIN ${sql}" 2>&1 | tee -a "${RUN_LOG}" || true
	fi
	local out rc=0
	if out=$(psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
		-c "SET threads = ${PERF_PK_THREADS};" \
		-c '\timing on' -c "${sql}" 2>&1); then
		rc=0
	else
		rc=$?
	fi
	printf '%s\n' "${out}" | tee -a "${RUN_LOG}"
	if [[ ${rc} -ne 0 ]]; then
		printf '!!! %s FAILED (psql exit %s)\n' "${label}" "${rc}" |
			tee -a "${RUN_LOG}"
	fi
	TIMINGS["${label}"]=$(extract_last_time_ms "${out}")
}

# Off the clock: no timing recorded, but a failure here is fatal -- every
# measurement below depends on it.
run_setup() {
	local label="$1" sql="$2"
	printf '\n--- setup: %s ---\n' "${label}" | tee -a "${RUN_LOG}"
	psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
		-c "SET threads = ${PERF_PK_THREADS};" -c "${sql}" 2>&1 |
		tee -a "${RUN_LOG}"
}

scalar() {
	psql "${PSQL_CONN}" -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}

# Memory, read off the server process rather than measured inside it: what a
# load holds of the committed-but-unpublished window is a property of the
# process, not of any one statement.
#
# VmHWM is a high-water mark over the process's whole life, so without a reset
# every shape after the first would just report the parquet staging peak.
# Writing 5 to clear_refs drops it back to the current RSS (Documentation/
# filesystems/proc.rst), which is what makes a per-shape peak possible.
proc_field_mb() {
	[[ -n "${SERVER_PID}" ]] || return 0
	awk -v k="$1:" '$1 == k { printf "%.0f", $2 / 1024 }' \
		"/proc/${SERVER_PID}/status" 2>/dev/null || true
}
reset_peak_rss() {
	[[ -n "${SERVER_PID}" ]] || return 0
	echo 5 >"/proc/${SERVER_PID}/clear_refs" 2>/dev/null || true
}

# Held is read BEFORE the refresh that follows each load: publishing releases
# the window, so after it the number this is here to show is gone.
#
# Both numbers are RSS, so they carry whatever the allocator has not returned
# to the OS; jemalloc (SDB_ALLOC=JE) is in no hurry to. Read them as an upper
# bound, and compare them between runs, not against zero.
mem_record() {
	local label="$1"
	PEAK_RSS["${label}"]="$(proc_field_mb VmHWM)"
	HELD_RSS["${label}"]="$(proc_field_mb VmRSS)"
}

start_serened

# ---------------------------------------------------------------------------
# Source staging, all off the clock. The parquet is read once into a
# transactional table carrying a row number, so each measured batch below is a
# range scan of local storage rather than another pass over the file. Identical
# in every binary, so it cannot distort the comparison.
# ---------------------------------------------------------------------------
SRC_WHERE=""
[[ -n "${PERF_PK_ROWS}" ]] && SRC_WHERE="LIMIT ${PERF_PK_ROWS}"
DEDUP=""
[[ "${PERF_PK_DEDUP}" == "1" ]] && DEDUP="QUALIFY row_number() OVER (PARTITION BY \"WatchID\") = 1"

run_setup "stage source" "
CREATE TABLE src AS
SELECT row_number() OVER () AS rn, ${COL_LIST}
FROM (
  SELECT ${SRC_SELECT}
  FROM read_parquet('${PARQUET_FILE}')
  ${DEDUP}
  ${SRC_WHERE}
) s;"

SRC_ROWS=$(scalar 'SELECT count(*) FROM src;')
SRC_KEYS=$(scalar 'SELECT count(DISTINCT watchid) FROM src;')
printf '\nstaged rows=%s distinct keys=%s\n' "${SRC_ROWS}" "${SRC_KEYS}" |
	tee -a "${RUN_LOG}"
if [[ "${SRC_ROWS}" != "${SRC_KEYS}" ]]; then
	echo "!!! source has duplicate keys -- the PK load will fail." |
		tee -a "${RUN_LOG}"
	echo "    re-run with PERF_PK_DEDUP=1, or use a sample that is unique." |
		tee -a "${RUN_LOG}"
	exit 1
fi

# The floor the memory numbers sit on: the source staged, no search table
# loaded yet. Identical work in every run, so it subtracts out.
mem_record staged

# ---------------------------------------------------------------------------
# Shape 1: one transaction. Measures the statement-end probe; the commit passes
# run once and are lost in the noise.
# ---------------------------------------------------------------------------
run_setup "create pk_single" "
CREATE TABLE pk_single (${COL_DEFS}, PRIMARY KEY (${PERF_PK_COLUMNS}))
  WITH ($(table_opts));"
reset_peak_rss
run_sql "single_txn_pk_insert" \
	"INSERT INTO pk_single SELECT ${COL_LIST} FROM src;"
mem_record single_txn_pk
run_sql "single_txn_pk_refresh" "VACUUM (REFRESH_TABLE) pk_single;"

if [[ "${PERF_PK_CONTROL}" == "1" ]]; then
	run_setup "create nopk_single" "
CREATE TABLE nopk_single (${COL_DEFS}) WITH ($(table_opts));"
	reset_peak_rss
	run_sql "single_txn_nopk_insert" \
		"INSERT INTO nopk_single SELECT ${COL_LIST} FROM src;"
	mem_record single_txn_nopk
	run_sql "single_txn_nopk_refresh" "VACUUM (REFRESH_TABLE) nopk_single;"
fi

# Last in the shape, so the search-table pair above is measured under the
# conditions it always was. No refresh: a transactional table publishes as it
# commits, so there is nothing to wait for and no counterpart to time.
if [[ "${PERF_PK_REFERENCE}" == "1" ]]; then
	run_setup "create txn_pk_single" "
CREATE TABLE txn_pk_single (${COL_DEFS}, PRIMARY KEY (${PERF_PK_COLUMNS}));"
	run_sql "single_txn_txnpk_insert" \
		"INSERT INTO txn_pk_single SELECT ${COL_LIST} FROM src;"
	run_setup "create txn_nopk_single" "
CREATE TABLE txn_nopk_single (${COL_DEFS});"
	run_sql "single_txn_txnnopk_insert" \
		"INSERT INTO txn_nopk_single SELECT ${COL_LIST} FROM src;"
fi

# ---------------------------------------------------------------------------
# Shape 2: many transactions. One autocommit INSERT per batch, so the commit
# passes run PERF_PK_BATCHES times. Timed as a whole -- per-statement psql
# timings would be swamped by connection setup.
# ---------------------------------------------------------------------------
PER_BATCH=$(((SRC_ROWS + PERF_PK_BATCHES - 1) / PERF_PK_BATCHES))

# Runs PERF_PK_BATCHES statements over successive row-number ranges of the
# staged source, one autocommit statement each, timed as a whole. `template`
# takes the range predicate appended to it.
#
# Each range is a separate scan of `src`, so a floor measurement (the same
# ranges, counting instead of inserting) is taken alongside and reported: the
# batched insert minus that floor is what the per-commit path actually costs.
batched_run() {
	local label="$1" template="$2"
	printf '\n=== %s (%s batches of %s) ===\n' \
		"${label}" "${PERF_PK_BATCHES}" "${PER_BATCH}" | tee -a "${RUN_LOG}"
	local start end rc=0
	start=$(date +%s.%N)
	local -a args=(-v ON_ERROR_STOP=1 -X -c "SET threads = ${PERF_PK_THREADS};")
	local i lo hi
	for ((i = 0; i < PERF_PK_BATCHES; i++)); do
		lo=$((i * PER_BATCH))
		hi=$((lo + PER_BATCH))
		args+=(-c "${template} WHERE rn > ${lo} AND rn <= ${hi};")
	done
	# `if psql`, not `if ! psql`: after a negation $? is the negation's status,
	# which is 0 exactly when psql failed, so the failure would be reported as
	# success and the summary would show a timing for a load that did nothing.
	if psql "${PSQL_CONN}" "${args[@]}" >>"${RUN_LOG}" 2>&1; then
		rc=0
	else
		rc=$?
	fi
	end=$(date +%s.%N)
	if [[ ${rc} -ne 0 ]]; then
		printf '!!! %s FAILED (psql exit %s)\n' "${label}" "${rc}" |
			tee -a "${RUN_LOG}"
		TIMINGS["${label}"]=""
		return
	fi
	TIMINGS["${label}"]=$(awk -v a="${start}" -v b="${end}" \
		'BEGIN{printf "%.3f", (b-a)*1000}')
	printf 'elapsed: %s ms\n' "${TIMINGS[${label}]}" | tee -a "${RUN_LOG}"
}

# The floor: the same statements, same ranges, reading the source and writing
# nothing. Whatever this costs is scan and statement overhead, present in every
# batched number below and in every binary.
batched_run many_txn_scan_floor "SELECT count(*) FROM src"

run_setup "create pk_batched" "
CREATE TABLE pk_batched (${COL_DEFS}, PRIMARY KEY (${PERF_PK_COLUMNS}))
  WITH ($(table_opts));"
reset_peak_rss
batched_run many_txn_pk_insert \
	"INSERT INTO pk_batched SELECT ${COL_LIST} FROM src"
mem_record many_txn_pk
run_sql "many_txn_pk_refresh" "VACUUM (REFRESH_TABLE) pk_batched;"

if [[ "${PERF_PK_CONTROL}" == "1" ]]; then
	run_setup "create nopk_batched" "
CREATE TABLE nopk_batched (${COL_DEFS}) WITH ($(table_opts));"
	reset_peak_rss
	batched_run many_txn_nopk_insert \
		"INSERT INTO nopk_batched SELECT ${COL_LIST} FROM src"
	mem_record many_txn_nopk
	run_sql "many_txn_nopk_refresh" "VACUUM (REFRESH_TABLE) nopk_batched;"
fi

if [[ "${PERF_PK_REFERENCE}" == "1" ]]; then
	run_setup "create txn_pk_batched" "
CREATE TABLE txn_pk_batched (${COL_DEFS}, PRIMARY KEY (${PERF_PK_COLUMNS}));"
	batched_run many_txn_txnpk_insert \
		"INSERT INTO txn_pk_batched SELECT ${COL_LIST} FROM src"
	run_setup "create txn_nopk_batched" "
CREATE TABLE txn_nopk_batched (${COL_DEFS});"
	batched_run many_txn_txnnopk_insert \
		"INSERT INTO txn_nopk_batched SELECT ${COL_LIST} FROM src"
fi

# ---------------------------------------------------------------------------
# Correctness, not speed: a load that silently dropped or duplicated rows would
# make every number above meaningless.
# ---------------------------------------------------------------------------
PK_SINGLE_ROWS=$(scalar 'SELECT count(*) FROM pk_single;')
PK_BATCHED_ROWS=$(scalar 'SELECT count(*) FROM pk_batched;')
TXN_PK_SINGLE_ROWS="${SRC_ROWS}"
TXN_PK_BATCHED_ROWS="${SRC_ROWS}"
if [[ "${PERF_PK_REFERENCE}" == "1" ]]; then
	TXN_PK_SINGLE_ROWS=$(scalar 'SELECT count(*) FROM txn_pk_single;')
	TXN_PK_BATCHED_ROWS=$(scalar 'SELECT count(*) FROM txn_pk_batched;')
fi

fmt_ms() {
	[[ -n "${1:-}" ]] && awk -v v="$1" 'BEGIN{printf "%.0f ms", v}' || printf 'n/a'
}
fmt_mb() {
	[[ -n "${1:-}" ]] && printf '%s MB' "$1" || printf 'n/a'
}
ratio() {
	[[ -n "${1:-}" && -n "${2:-}" && "${2:-0}" != "0" ]] &&
		awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2fx", a/b}' || printf '-'
}
sub() {
	[[ -n "${1:-}" && -n "${2:-}" ]] &&
		awk -v a="$1" -v b="$2" 'BEGIN{printf "%.3f", a-b}' || printf ''
}

{
	echo
	echo "======== SEARCH TABLE PRIMARY KEY ========"
	echo "binary:  ${SERENED_BIN}"
	echo "key:     ${PERF_PK_COLUMNS}"
	echo "rows:    ${SRC_ROWS} (all keys distinct)"
	echo "options: $(table_opts)"
	echo "batches: ${PERF_PK_BATCHES}"
	echo
	printf "%-28s %13s %13s   %7s\n" "shape" "pk" "no pk" "pk/no"
	printf "%-28s %13s %13s   %7s\n" "----------------------------" \
		"-------------" "-------------" "-------"
	printf "%-28s %13s %13s   %7s\n" "one transaction: insert" \
		"$(fmt_ms "${TIMINGS[single_txn_pk_insert]:-}")" \
		"$(fmt_ms "${TIMINGS[single_txn_nopk_insert]:-}")" \
		"$(ratio "${TIMINGS[single_txn_pk_insert]:-}" "${TIMINGS[single_txn_nopk_insert]:-}")"
	printf "%-28s %13s %13s   %7s\n" "one transaction: refresh" \
		"$(fmt_ms "${TIMINGS[single_txn_pk_refresh]:-}")" \
		"$(fmt_ms "${TIMINGS[single_txn_nopk_refresh]:-}")" \
		"$(ratio "${TIMINGS[single_txn_pk_refresh]:-}" "${TIMINGS[single_txn_nopk_refresh]:-}")"
	printf "%-28s %13s %13s   %7s\n" "${PERF_PK_BATCHES} transactions: insert" \
		"$(fmt_ms "${TIMINGS[many_txn_pk_insert]:-}")" \
		"$(fmt_ms "${TIMINGS[many_txn_nopk_insert]:-}")" \
		"$(ratio "${TIMINGS[many_txn_pk_insert]:-}" "${TIMINGS[many_txn_nopk_insert]:-}")"
	printf "%-28s %13s %13s   %7s\n" "${PERF_PK_BATCHES} transactions: refresh" \
		"$(fmt_ms "${TIMINGS[many_txn_pk_refresh]:-}")" \
		"$(fmt_ms "${TIMINGS[many_txn_nopk_refresh]:-}")" \
		"$(ratio "${TIMINGS[many_txn_pk_refresh]:-}" "${TIMINGS[many_txn_nopk_refresh]:-}")"
	echo
	floor="${TIMINGS[many_txn_scan_floor]:-}"
	printf "%-28s %13s\n" "${PERF_PK_BATCHES} transactions: floor" \
		"$(fmt_ms "${floor}")"
	printf "%-28s %13s %13s\n" "  minus floor" \
		"$(fmt_ms "$(sub "${TIMINGS[many_txn_pk_insert]:-}" "${floor}")")" \
		"$(fmt_ms "$(sub "${TIMINGS[many_txn_nopk_insert]:-}" "${floor}")")"
	echo "The floor is the same statements reading the source and writing"
	echo "nothing: scan and per-statement overhead, which the batched inserts"
	echo "also pay. Read the batched comparison off the minus-floor row."
	if [[ "${PERF_PK_REFERENCE}" == "1" ]]; then
		echo
		printf "%-28s %13s %13s   %7s\n" "reference: plain table" "pk" "no pk" \
			"pk/no"
		printf "%-28s %13s %13s   %7s\n" "----------------------------" \
			"-------------" "-------------" "-------"
		printf "%-28s %13s %13s   %7s\n" "one transaction: insert" \
			"$(fmt_ms "${TIMINGS[single_txn_txnpk_insert]:-}")" \
			"$(fmt_ms "${TIMINGS[single_txn_txnnopk_insert]:-}")" \
			"$(ratio "${TIMINGS[single_txn_txnpk_insert]:-}" "${TIMINGS[single_txn_txnnopk_insert]:-}")"
		printf "%-28s %13s %13s   %7s\n" "${PERF_PK_BATCHES} transactions: insert" \
			"$(fmt_ms "${TIMINGS[many_txn_txnpk_insert]:-}")" \
			"$(fmt_ms "${TIMINGS[many_txn_txnnopk_insert]:-}")" \
			"$(ratio "${TIMINGS[many_txn_txnpk_insert]:-}" "${TIMINGS[many_txn_txnnopk_insert]:-}")"
		printf "%-28s %13s %13s\n" "  minus floor" \
			"$(fmt_ms "$(sub "${TIMINGS[many_txn_txnpk_insert]:-}" "${floor}")")" \
			"$(fmt_ms "$(sub "${TIMINGS[many_txn_txnnopk_insert]:-}" "${floor}")")"
		echo "The same rows and statements in an ordinary table, where the key"
		echo "is an ART index over local storage. Its pk/no is the whole cost of"
		echo "enforcing a key there and carries no term-indexing, which is what"
		echo "the search table's own pk/no conflates. A yardstick, not a target:"
		echo "the two storages answer different questions."
	fi
	echo
	printf "%-28s %13s %13s\n" "memory (RSS)" "pk" "no pk"
	printf "%-28s %13s %13s\n" "----------------------------" \
		"-------------" "-------------"
	printf "%-28s %13s\n" "after staging: held" \
		"$(fmt_mb "${HELD_RSS[staged]:-}")"
	printf "%-28s %13s %13s\n" "one transaction: peak" \
		"$(fmt_mb "${PEAK_RSS[single_txn_pk]:-}")" \
		"$(fmt_mb "${PEAK_RSS[single_txn_nopk]:-}")"
	printf "%-28s %13s %13s\n" "one transaction: held" \
		"$(fmt_mb "${HELD_RSS[single_txn_pk]:-}")" \
		"$(fmt_mb "${HELD_RSS[single_txn_nopk]:-}")"
	printf "%-28s %13s %13s\n" "${PERF_PK_BATCHES} transactions: peak" \
		"$(fmt_mb "${PEAK_RSS[many_txn_pk]:-}")" \
		"$(fmt_mb "${PEAK_RSS[many_txn_nopk]:-}")"
	printf "%-28s %13s %13s\n" "${PERF_PK_BATCHES} transactions: held" \
		"$(fmt_mb "${HELD_RSS[many_txn_pk]:-}")" \
		"$(fmt_mb "${HELD_RSS[many_txn_nopk]:-}")"
	echo "Peak is VmHWM since that shape started, the mark being reset before"
	echo "each; held is VmRSS at the end of the load, read before the refresh"
	echo "that publishes it. Held is where whatever a load keeps of the"
	echo "unpublished window shows up, and is the number to compare between"
	echo "runs; peak also carries what the load itself needed. Both are RSS, so"
	echo "they include whatever jemalloc has not yet returned to the OS."
	echo
	echo "rows loaded: pk_single=${PK_SINGLE_ROWS} pk_batched=${PK_BATCHED_ROWS}"
	if [[ "${PERF_PK_REFERENCE}" == "1" ]]; then
		echo "             txn_pk_single=${TXN_PK_SINGLE_ROWS}" \
			"txn_pk_batched=${TXN_PK_BATCHED_ROWS}"
	fi
	echo "(each must equal ${SRC_ROWS})"
	echo
	echo "The pk/no column conflates certification with term-indexing the key"
	echo "column, which a table without a PK does not do. For certification"
	echo "alone, compare the pk column across runs: this binary against one"
	echo "where the key is declared and inert (main)."
	echo "=========================================="
} | tee -a "${RUN_LOG}"

echo
echo "log: ${RUN_LOG}"
echo "server log: ${LOG}"

# A load that dropped rows makes every timing above a measurement of nothing,
# so it fails the run rather than printing a number somebody might read.
if [[ "${PK_SINGLE_ROWS}" != "${SRC_ROWS}" ||
	"${PK_BATCHED_ROWS}" != "${SRC_ROWS}" ||
	"${TXN_PK_SINGLE_ROWS}" != "${SRC_ROWS}" ||
	"${TXN_PK_BATCHED_ROWS}" != "${SRC_ROWS}" ]]; then
	echo "FAILED: expected ${SRC_ROWS} rows in each PK table" >&2
	exit 1
fi
