#!/usr/bin/env bash
# Compare iresearch-columnstore (PG INCLUDE) vs DuckDB-native table scan on
# nested-typed columns (ARRAY, LIST, STRUCT, MAP).
#
# Every query is a pure full-table scan + aggregate -- no WHERE, no
# ORDER BY LIMIT, no JOIN. DuckDB pushes those past the scan, we don't,
# so they're excluded so the comparison stays apples-to-apples on the
# scan + materialisation path alone.
#
# The data is generated synthetically from range(N) so the script is
# self-contained; no external parquet file required.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
SERENED_DATA_DIR="${PERF_SERENED_DATA_DIR:-${RESULTS_DIR}/nested_perf_data}"
NATIVE_DB="${PERF_NATIVE_DB:-${RESULTS_DIR}/nested_perf_native.duckdb}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
PORT="${PERF_PORT:-6263}"
LOG="/tmp/${USER}-serened-nested-perf.log"
N="${PERF_NESTED_ROWS:-1000000}"

if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN} -- build the perf binary first" >&2
	echo "(override with PERF_BUILD_DIR=...)" >&2
	exit 1
fi

mkdir -p "${RESULTS_DIR}"
RUN_LOG="${RESULTS_DIR}/nested-$(date -u +%Y%m%dT%H%M%SZ).log"

declare -A TIMINGS=()

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

BUILD_THREADS="${PERF_BUILD_THREADS:-$(nproc)}"
SCAN_THREADS="${PERF_SCAN_THREADS:-1}"
PERF_EXPLAIN="${PERF_EXPLAIN:-1}"
SEARCH_PATH_SQL="SET search_path TO public, native_db.main;"

extract_last_time_ms() {
	awk '/^Time: /{t=$2} END{if (t!="") printf "%s", t}' <<<"$1"
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

NDB_SQL_PATH=$(printf '%s' "${NATIVE_DB}" | sed "s/'/''/g")

run_setup "attach_native_db" "${BUILD_THREADS}" "
ATTACH '${NDB_SQL_PATH}' AS native_db (TYPE duckdb, STORAGE_VERSION latest);
SET search_path TO public, native_db.main;
"

# --- 1. Schema + data --------------------------------------------------------
# All four nested columns are INCLUDE-only -- no text/posting machinery is
# exercised. `inverted()` with no indexed columns is accepted: the catalog
# auto-injects the table's PK as an implicit numeric-tokenised column to
# drive per-row doc allocation in the sink.

run_sql "create_table_serened" "${BUILD_THREADS}" "
CREATE TABLE nested_src (
  pk INTEGER PRIMARY KEY,
  vec FLOAT[3],
  tags INTEGER[],
  meta STRUCT(a INTEGER, b VARCHAR, c FLOAT),
  attrs MAP(VARCHAR, INTEGER)
);"

# Generate N deterministic rows from range(). The values are chosen so each
# nested column gets a non-trivial mix and aggregates have a wide range.
run_sql "insert_serened" "${BUILD_THREADS}" "
INSERT INTO nested_src
SELECT i AS pk,
       [(i % 100)::FLOAT, ((i * 7) % 100)::FLOAT, ((i * 13) % 100)::FLOAT]::FLOAT[3] AS vec,
       [(i + 0) % 50, (i + 1) % 50, (i + 2) % 50, (i + 3) % 50] AS tags,
       ROW(i * 2, 'b-' || (i % 100)::VARCHAR, (i * 0.1)::FLOAT)
         ::STRUCT(a INTEGER, b VARCHAR, c FLOAT) AS meta,
       MAP {'k1': i, 'k2': i * 2, 'k3': i * 3} AS attrs
FROM range(${N}) t(i);"

run_sql "create_native_table" "${BUILD_THREADS}" "
CREATE TABLE native_db.main.nested_native AS
SELECT * FROM nested_src;"

NATIVE_SIZE_PRE_MAIN=0
NATIVE_SIZE_PRE_WAL=0
[[ -f "${NATIVE_DB}" ]] && NATIVE_SIZE_PRE_MAIN=$(du -sb "${NATIVE_DB}" | awk '{print $1}')
[[ -f "${NATIVE_DB}.wal" ]] && NATIVE_SIZE_PRE_WAL=$(du -sb "${NATIVE_DB}.wal" | awk '{print $1}')

run_setup "checkpoint_native_db" "${BUILD_THREADS}" "
CHECKPOINT native_db;
"

run_sql "create_index" "${BUILD_THREADS}" "
CREATE INDEX nested_idx ON nested_src USING inverted()
  INCLUDE (vec, tags, meta, attrs);"

# --- 2. Benchmarks -----------------------------------------------------------
# Each query is a full-table scan + aggregate that forces materialisation of
# the whole nested column. No predicate, no LIMIT, no JOIN -- so DuckDB's
# pushdown advantage doesn't apply.

run_sql "bench_count_indexed" "${SCAN_THREADS}" \
	"SELECT COUNT(*) FROM nested_idx;"
run_sql "bench_count_native" "${SCAN_THREADS}" \
	"SELECT COUNT(*) FROM nested_native;"

# ARRAY<FLOAT, 3>: sum every element of every row.
run_sql "bench_array_indexed" "${SCAN_THREADS}" \
	"SELECT SUM(vec[1] + vec[2] + vec[3]) FROM nested_idx;"
run_sql "bench_array_native" "${SCAN_THREADS}" \
	"SELECT SUM(vec[1] + vec[2] + vec[3]) FROM nested_native;"

# LIST<INTEGER>: sum every list's elements.
run_sql "bench_list_indexed" "${SCAN_THREADS}" \
	"SELECT SUM(list_sum(tags)) FROM nested_idx;"
run_sql "bench_list_native" "${SCAN_THREADS}" \
	"SELECT SUM(list_sum(tags)) FROM nested_native;"

# STRUCT<a INT, b VARCHAR, c FLOAT>: aggregate all three fields so neither
# side can field-prune the scan.
run_sql "bench_struct_indexed" "${SCAN_THREADS}" \
	"SELECT SUM(meta.a), SUM(length(meta.b)), SUM(meta.c) FROM nested_idx;"
run_sql "bench_struct_native" "${SCAN_THREADS}" \
	"SELECT SUM(meta.a), SUM(length(meta.b)), SUM(meta.c) FROM nested_native;"

# MAP<VARCHAR, INTEGER>: sum every value and every key length so neither
# the keys nor the values side can be pruned.
run_sql "bench_map_indexed" "${SCAN_THREADS}" "
SELECT SUM(list_sum(map_values(attrs))),
       SUM(list_sum(list_transform(map_keys(attrs), k -> length(k))))
FROM nested_idx;"
run_sql "bench_map_native" "${SCAN_THREADS}" "
SELECT SUM(list_sum(map_values(attrs))),
       SUM(list_sum(list_transform(map_keys(attrs), k -> length(k))))
FROM nested_native;"

# --- 3. Storage size ---------------------------------------------------------
sum_ext() {
	local dir="$1" ext="$2"
	find "${dir}" -name "*.${ext}" -printf '%s\n' 2>/dev/null |
		awk '{s+=$1} END{printf "%d\n", s+0}'
}
sum_glob() {
	find "$@" -printf '%s\n' 2>/dev/null |
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

total=$(du -sb "${SERENED_DATA_DIR}" | awk '{print $1}')
ndb=0
ndb_wal=0
[[ -f "${NATIVE_DB}" ]] && ndb=$(du -sb "${NATIVE_DB}" | awk '{print $1}')
[[ -f "${NATIVE_DB}.wal" ]] && ndb_wal=$(du -sb "${NATIVE_DB}.wal" | awk '{print $1}')
ndb_post_total=$((ndb + ndb_wal))
ndb_pre_total=$((NATIVE_SIZE_PRE_MAIN + NATIVE_SIZE_PRE_WAL))

{
	echo
	echo "=== storage_size ==="
	printf "duckdb native pre-ckpt:   %12d bytes (%s) [main=%s wal=%s]\n" \
		"${ndb_pre_total}" "$(human "${ndb_pre_total}")" \
		"$(human "${NATIVE_SIZE_PRE_MAIN}")" "$(human "${NATIVE_SIZE_PRE_WAL}")"
	printf "duckdb native post-ckpt:  %12d bytes (%s) [main=%s wal=%s]\n" \
		"${ndb_post_total}" "$(human "${ndb_post_total}")" \
		"$(human "${ndb}")" "$(human "${ndb_wal}")"
	printf "serened data dir:         %12d bytes (%s)\n" \
		"${total}" "$(human "${total}")"
	declare -a exts=(doc pos pay cs csd csi ti tm sm)
	for ext in "${exts[@]}"; do
		s=$(sum_ext "${SERENED_DATA_DIR}" "${ext}")
		printf "  .%-7s %14d bytes (%s)\n" "${ext}" "${s}" "$(human "${s}")"
	done
	seg_total=$(sum_glob "${SERENED_DATA_DIR}" -name 'segments_*' -type f)
	printf "  %-8s %14d bytes (%s)\n" "segments_*" \
		"${seg_total}" "$(human "${seg_total}")"
	rdb_total=$(sum_glob "${SERENED_DATA_DIR}/engine_rocksdb" -type f)
	printf "  %-8s %14d bytes (%s)\n" "rocksdb" \
		"${rdb_total}" "$(human "${rdb_total}")"
} | tee -a "${RUN_LOG}"

# --- 4. Summary --------------------------------------------------------------
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
	echo "rows:       ${N}"
	echo "build threads: ${BUILD_THREADS}    scan threads: ${SCAN_THREADS}"
	echo
	printf "%-20s %12s\n" "phase" "time"
	printf "%-20s %12s\n" "--------------------" "------------"
	printf "%-20s %12s\n" "insert_serened" "$(fmt_ms "${TIMINGS[insert_serened]:-}")"
	printf "%-20s %12s\n" "create_native_table" "$(fmt_ms "${TIMINGS[create_native_table]:-}")"
	printf "%-20s %12s\n" "checkpoint_native" "$(fmt_ms "${TIMINGS[checkpoint_native_db]:-}")"
	printf "%-20s %12s\n" "create_index" "$(fmt_ms "${TIMINGS[create_index]:-}")"
	echo
	printf "%-22s %12s %12s   %s\n" "query" "indexed (cs)" "duckdb native" "speedup vs native"
	printf "%-22s %12s %12s   %s\n" "----------------------" "------------" "------------" "-----------------"
	for q in count array list struct map; do
		i="${TIMINGS[bench_${q}_indexed]:-}"
		n="${TIMINGS[bench_${q}_native]:-}"
		printf "%-22s %12s %12s   %s\n" \
			"${q}" "$(fmt_ms "${i}")" "$(fmt_ms "${n}")" \
			"$(ratio "${n}" "${i}")"
	done
	echo
	cs_total=$(sum_ext "${SERENED_DATA_DIR}" "cs")
	rdb_total=$(sum_glob "${SERENED_DATA_DIR}/engine_rocksdb" -type f)
	printf "%-26s %12s\n" "storage" "bytes"
	printf "%-26s %12s\n" "--------------------------" "------------"
	printf "%-26s %12s (%s)\n" "duckdb native (post-ckpt)" \
		"${ndb_post_total}" "$(human "${ndb_post_total}")"
	printf "%-26s %12s (%s)\n" "serened cs only (.cs)" \
		"${cs_total}" "$(human "${cs_total}")"
	printf "%-26s %12s (%s)\n" "serened rocksdb (base tbl)" \
		"${rdb_total}" "$(human "${rdb_total}")"
	printf "%-26s %12s (%s)\n" "serened data dir total" \
		"${total}" "$(human "${total}")"
	echo "========================================="
} | tee -a "${RUN_LOG}"

echo
echo "log: ${RUN_LOG}"
echo "server log: ${LOG}"
