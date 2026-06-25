#!/usr/bin/env bash
# bench_pk_lookup.sh -- focused benchmark for PK fetch latency through the
# new typed columnstore.
#
# What it actually measures: the cost of *streaming scan with per-doc PK
# resolution*. The query shape forces the path that exercises
# SegmentPkIterator (new columnstore BLOB lookup under
# kGeneratedPKId) once per matched doc: a filter that matches every row
# plus a projected column that isn't INCLUDE'd, so each doc has to
# resolve its PK and feed IndexSource for the materialisation.
#
# Pre-reqs: build_perf binary, no other serened on PERF_PK_PORT.
#
# Storage-size comparison (legacy-STORE removal: PK out of .csd/.csi,
# into .cs) is best read off run_hits_perf.sh's existing storage
# breakdown -- this script keeps its scope to lookup speed.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
RESULTS_DIR="${ROOT}/scripts/perf/results"
DATA_DIR="${PERF_PK_DATA_DIR:-${RESULTS_DIR}/pk_perf_data}"
NATIVE_DB="${PERF_PK_NATIVE_DB:-${RESULTS_DIR}/pk_perf_native.duckdb}"
PORT="${PERF_PK_PORT:-6464}"
LOG="/tmp/${USER}-serened-pk-bench.log"

ROW_COUNT="${PERF_PK_ROWS:-100000}"

if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN} -- build the perf binary first" >&2
	exit 1
fi

mkdir -p "${RESULTS_DIR}"
RUN_LOG="${RESULTS_DIR}/pk-bench-$(date -u +%Y%m%dT%H%M%SZ).log"

# Fresh state.
killall -9 serened >/dev/null 2>&1 || true
sleep 1
rm -rf "${DATA_DIR}"
rm -f "${NATIVE_DB}" "${NATIVE_DB}.wal"

echo "starting ${SERENED_BIN} on port ${PORT} (rows=${ROW_COUNT})" |
	tee -a "${RUN_LOG}"
"${SERENED_BIN}" "${DATA_DIR}" \
	--listen "postgres://0.0.0.0:${PORT}" \
	>"${LOG}" 2>&1 &
SERENED_PID=$!
trap "kill -9 ${SERENED_PID} >/dev/null 2>&1 || true" EXIT

PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"
for _ in $(seq 1 30); do
	if psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
		break
	fi
	sleep 0.5
done
if ! psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
	echo "serened did not come up:" >&2
	tail -30 "${LOG}" >&2
	exit 1
fi

# Per-section timing helper. Captures the LAST `Time: <ms>` line so we can
# print a clean summary table at the end.
declare -A TIMINGS=()
extract_last_time_ms() {
	awk '/^Time: /{t=$2} END{if (t!="") printf "%s", t}' <<<"$1"
}
run_sql() {
	local label="$1" threads="$2" sql="$3"
	local explain="${4:-}" # optional "explain" -> prepend EXPLAIN before timing
	printf '\n=== %s (threads=%s) ===\n' "${label}" "${threads}" |
		tee -a "${RUN_LOG}"
	if [[ "${explain}" == "explain" ]]; then
		psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
			-c '\timing off' \
			-c "SET threads = ${threads};" \
			-c "EXPLAIN ${sql}" 2>&1 | tee -a "${RUN_LOG}" || true
	fi
	local out rc=0
	# Capture exit code separately from `set -e` so we can print the psql
	# error message before bailing -- otherwise command-substitution swallows
	# the output and the user sees only an empty prompt return.
	out=$(psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
		-c "SET threads = ${threads};" \
		-c '\timing on' \
		-c "${sql}" 2>&1) || rc=$?
	printf '%s\n' "${out}" | tee -a "${RUN_LOG}"
	TIMINGS["${label}"]=$(extract_last_time_ms "${out}")
	if [[ "${rc}" -ne 0 ]]; then
		echo "${label}: psql exited with ${rc}" | tee -a "${RUN_LOG}" >&2
		exit "${rc}"
	fi
}

NDB_SQL_PATH=$(printf '%s' "${NATIVE_DB}" | sed "s/'/''/g")
BUILD_THREADS="${PERF_PK_BUILD_THREADS:-$(nproc)}"
SCAN_THREADS="${PERF_PK_SCAN_THREADS:-1}"

# 1. Schema + dataset on the serened side.
#    `body` is a tokenisable VARCHAR with one common token "row" present
#    in every row -- guarantees the bench query matches every doc and
#    exercises the per-doc PK path for the entire scan.
#    `val` is BIGINT, NOT INCLUDE'd in the index, so projecting it
#    forces IndexSource to fetch each row through its PK -- which is
#    what we want to measure.
run_sql "create_table_serened" "${BUILD_THREADS}" "
CREATE TABLE pk_bench (
  pk BIGINT PRIMARY KEY,
  body VARCHAR,
  val BIGINT
);"
run_sql "populate_serened" "${BUILD_THREADS}" "
INSERT INTO pk_bench
SELECT i AS pk,
       'row ' || i AS body,
       i * 7 AS val
FROM range(0, ${ROW_COUNT}) t(i);"
run_sql "create_text_search_dict" "${BUILD_THREADS}" "
CREATE TEXT SEARCH DICTIONARY pk_dict(
  template = 'text', locale = 'en_US.UTF-8', case = 'none',
  stemming = false, accent = false, frequency = true, position = true);"
# `val` is INCLUDE'd so it's projectable from `pk_bench_idx`. The bench
# query below uses ts_phrase('row') -- the dict is configured for
# Positions+Frequency above so phrases work; a single token that's in
# every doc keeps the streaming-scan loop iterating over all N rows, which
# is the path that exercises SegmentPkIterator::seek per doc. The PK
# bytes accumulated by run_scan are unused downstream when every real
# projection is INCLUDE'd (cs_projections handles `val`), so the bench
# isolates pure per-doc PK fetch cost. A followup TODO covers skipping
# the PK collect when has_external_projections is false.
# Note: SUM(val) is used instead of COUNT(val) -- COUNT(<col>) with a
# WHERE on the index virtual table currently hits a planner-side bind
# error ("Failed to bind column reference") that's unrelated to the PK
# path. SUM produces the same shape (one aggregate over `val`) without
# tripping it.
run_sql "create_index" "${BUILD_THREADS}" "
CREATE INDEX pk_bench_idx ON pk_bench USING inverted(body pk_dict)
INCLUDE (val);"

# 2. Native baseline -- ATTACH a fresh duckdb db, CTAS the same shape.
run_sql "attach_native_db" "${BUILD_THREADS}" "
ATTACH '${NDB_SQL_PATH}' AS native_db (TYPE duckdb, STORAGE_VERSION latest);"
run_sql "create_native_table" "${BUILD_THREADS}" "
CREATE TABLE native_db.main.pk_native AS
SELECT * FROM pk_bench;"
run_sql "checkpoint_native" "${BUILD_THREADS}" "
CHECKPOINT native_db;"

# 3. Bench: streaming scan with PK projection.
#    serened side: every doc matches the match-all-shaped phrase, every
#    matched doc projects `val` (not INCLUDE'd) -- so SegmentPkIterator
#    runs once per doc and IndexSource resolves `val` via the PK.
#    Compared to a native sequential scan of the same column. The wall
#    clock divided by ROW_COUNT gives a per-doc PK fetch upper bound on
#    serened.
run_sql "bench_scan_with_pk_indexed" "${SCAN_THREADS}" "
SELECT SUM(val) FROM pk_bench_idx WHERE body @@ ts_phrase('row');" \
	explain
run_sql "bench_scan_with_pk_native" "${SCAN_THREADS}" "
SELECT SUM(val) FROM native_db.main.pk_native;" \
	explain

# 4. Storage size.
sum_glob() {
	find "$@" -printf '%s\n' 2>/dev/null |
		awk '{s+=$1} END{printf "%d\n", s+0}'
}
sum_ext() {
	find "$1" -name "*.$2" -printf '%s\n' 2>/dev/null |
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

ndb=0
[[ -f "${NATIVE_DB}" ]] && ndb=$(sum_glob "${NATIVE_DB}")
ndb_wal=0
[[ -f "${NATIVE_DB}.wal" ]] && ndb_wal=$(sum_glob "${NATIVE_DB}.wal")
total=$(sum_glob "${DATA_DIR}" -type f)

{
	echo
	echo "=== storage_size ==="
	printf "duckdb native:     %12d bytes (%s) [main=%s wal=%s]\n" \
		"$((ndb + ndb_wal))" "$(human "$((ndb + ndb_wal))")" \
		"$(human "${ndb}")" "$(human "${ndb_wal}")"
	printf "serened data dir:  %12d bytes (%s)\n" "${total}" \
		"$(human "${total}")"
	declare -a exts=(doc pos pay cs csd csi ti tm sm)
	for ext in "${exts[@]}"; do
		s=$(sum_ext "${DATA_DIR}" "${ext}")
		printf "  .%-5s %14d bytes (%s)\n" "${ext}" "${s}" "$(human "${s}")"
	done
} | tee -a "${RUN_LOG}"

# 5. Headline summary.
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
per_row_us() {
	local total_ms="$1" rows="$2"
	if [[ -z "${total_ms}" || -z "${rows}" || "${rows}" -eq 0 ]]; then
		printf 'n/a'
		return
	fi
	awk -v t="${total_ms}" -v r="${rows}" \
		'BEGIN{ printf "%.3f µs", (t * 1000) / r }'
}
{
	echo
	echo "================ PK BENCH SUMMARY ================"
	echo "rows: ${ROW_COUNT}    build threads: ${BUILD_THREADS}    scan threads: ${SCAN_THREADS}"
	echo
	printf "%-22s %12s\n" "phase" "time"
	printf "%-22s %12s\n" "---------------------" "------------"
	printf "%-22s %12s\n" "populate_serened" \
		"$(fmt_ms "${TIMINGS[populate_serened]:-}")"
	printf "%-22s %12s\n" "create_index" \
		"$(fmt_ms "${TIMINGS[create_index]:-}")"
	printf "%-22s %12s\n" "create_native_table" \
		"$(fmt_ms "${TIMINGS[create_native_table]:-}")"
	printf "%-22s %12s\n" "checkpoint_native" \
		"$(fmt_ms "${TIMINGS[checkpoint_native]:-}")"
	echo
	printf "%-26s %12s %12s\n" "scan + pk projection" "total" "per row"
	printf "%-26s %12s %12s\n" "--------------------------" "------------" "------------"
	printf "%-26s %12s %12s\n" "indexed (SegmentPkIter)" \
		"$(fmt_ms "${TIMINGS[bench_scan_with_pk_indexed]:-}")" \
		"$(per_row_us "${TIMINGS[bench_scan_with_pk_indexed]:-}" "${ROW_COUNT}")"
	printf "%-26s %12s %12s\n" "duckdb native (seq scan)" \
		"$(fmt_ms "${TIMINGS[bench_scan_with_pk_native]:-}")" \
		"$(per_row_us "${TIMINGS[bench_scan_with_pk_native]:-}" "${ROW_COUNT}")"
	echo "=================================================="
} | tee -a "${RUN_LOG}"

echo
echo "log: ${RUN_LOG}"
echo "server log: ${LOG}"
