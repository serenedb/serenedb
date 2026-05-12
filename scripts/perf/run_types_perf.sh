#!/usr/bin/env bash
# Per-type cs-scan vs duckdb-native bench. Parquet is the source on both
# sides -- serened creates a VIEW over read_parquet and indexes it, so
# the rocksdb base-table cost from run_nested_perf.sh is gone. DuckDB
# CTAS's the same parquet into its native columnar storage.
#
# Every query is a full scan + aggregate (no WHERE / ORDER BY LIMIT /
# JOIN) so DuckDB's pushdown advantages do not apply. The work on both
# sides decodes the same codec-compressed chunks; any speed difference
# is in the scan/materialise wrapper.
#
# Runs two timed phases per query:
#  - cold: first hit on a fresh-from-setup server (segments not yet
#          opened, codec state cold; OS page cache may still be warm
#          from CREATE INDEX writes).
#  - hot:  3rd hit (after one discarded warmup) -- steady-state warm.
# Both per-side and across-side speedups are printed.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
PARQUET_DIR="${PERF_PARQUET_DIR:-${HOME}/data}"
# .duckdb stays in scripts/perf/results/ which is in .gitignore.  The
# synthetic source parquet lives next to ${HOME}/data/hits.parquet so
# every perf script shares one parquet directory.
SERENED_DATA_DIR="${PERF_SERENED_DATA_DIR:-${RESULTS_DIR}/types_perf_data}"
NATIVE_DB="${PERF_NATIVE_DB:-${RESULTS_DIR}/types_perf_native.duckdb}"
PARQUET_FILE="${PERF_PARQUET_FILE:-${PARQUET_DIR}/types_perf.parquet}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
PORT="${PERF_PORT:-6263}"
LOG="/tmp/${USER}-serened-types-perf.log"
N="${PERF_ROWS:-1000000}"

if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN} -- build the perf binary first" >&2
	exit 1
fi

mkdir -p "${RESULTS_DIR}" "${PARQUET_DIR}"
RUN_LOG="${RESULTS_DIR}/types-$(date -u +%Y%m%dT%H%M%SZ).log"

declare -A TIMINGS=()

start_server() {
	killall -9 serened >/dev/null 2>&1 || true
	sleep 1
	"${SERENED_BIN}" "${SERENED_DATA_DIR}" \
		--server.endpoint "pgsql+tcp://0.0.0.0:${PORT}" \
		--log.foreground-tty true \
		>"${LOG}" 2>&1 &
	disown
	for _ in $(seq 1 30); do
		if psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.5
	done
	echo "serened did not come up; tail of ${LOG}:" >&2
	tail -50 "${LOG}" >&2
	exit 1
}

PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"

BUILD_THREADS="${PERF_BUILD_THREADS:-$(nproc)}"
SCAN_THREADS="${PERF_SCAN_THREADS:-1}"
SEARCH_PATH_SQL="SET search_path TO public, native_db.main;"

extract_last_time_ms() {
	awk '/^Time: /{t=$2} END{if (t!="") printf "%s", t}' <<<"$1"
}

run_sql() {
	local label="$1" threads="$2" sql="$3"
	printf '\n=== %s (threads=%s) ===\n' "${label}" "${threads}" |
		tee -a "${RUN_LOG}"
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

# --- 1. Generate the parquet file ------------------------------------------
# Done on a transient serened that we throw away afterwards -- only the
# parquet file survives. This avoids landing the synthetic source data
# in the perf serened's rocksdb base store.
rm -rf "${SERENED_DATA_DIR}" ${RESULTS_DIR}/types_genparquet_data
rm -f "${NATIVE_DB}" "${NATIVE_DB}.wal" "${PARQUET_FILE}"

PQ_SQL_PATH=$(printf '%s' "${PARQUET_FILE}" | sed "s/'/''/g")

echo "generating ${PARQUET_FILE} (${N} rows) via temporary serened"
"${SERENED_BIN}" ${RESULTS_DIR}/types_genparquet_data \
	--server.endpoint "pgsql+tcp://0.0.0.0:${PORT}" \
	--log.foreground-tty true \
	>"${LOG}" 2>&1 &
disown
for _ in $(seq 1 30); do
	psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1 && break
	sleep 0.5
done

run_setup "generate_parquet" "${BUILD_THREADS}" "
COPY (
  SELECT i AS pk,
         ((i * 3) % 200 - 100)::TINYINT AS i8,
         ((i * 5) % 60000 - 30000)::SMALLINT AS i16,
         ((i * 7) % 1000000) AS i32,
         (i * 1009)::BIGINT AS i64,
         (((i * 13) % 1000) / 7.0)::FLOAT AS f32,
         (((i * 17) % 10000) / 13.0)::DOUBLE AS f64,
         'str-' || (i % 1024)::VARCHAR AS s,
         (i % 2 = 0) AS bool_col,
         [(i + 0) % 50, (i + 1) % 50, (i + 2) % 50]::INTEGER[3] AS arr_i32,
         [(i + 0) % 30, (i + 1) % 30, (i + 2) % 30, (i + 3) % 30] AS lst_i32,
         ROW(i * 2, 'b-' || (i % 100)::VARCHAR)
           ::STRUCT(a INTEGER, b VARCHAR) AS struct_basic,
         MAP {'k1': i, 'k2': i * 2, 'k3': i * 3} AS map_i32,
         [ROW('p1', i)::STRUCT(k VARCHAR, v INTEGER),
          ROW('p2', i * 2)::STRUCT(k VARCHAR, v INTEGER)] AS lst_struct,
         ROW('name-' || (i % 100)::VARCHAR,
             [ROW('k1', i)::STRUCT(k VARCHAR, v INTEGER),
              ROW('k2', i * 2)::STRUCT(k VARCHAR, v INTEGER)])
           ::STRUCT(name VARCHAR, vals STRUCT(k VARCHAR, v INTEGER)[]) AS deep
  FROM range(${N}) t(i)
) TO '${PQ_SQL_PATH}' (FORMAT parquet);
"

killall -9 serened >/dev/null 2>&1 || true
sleep 1
PQ_SIZE=$(du -sb "${PARQUET_FILE}" | awk '{print $1}')
human_pq=$(awk -v b="${PQ_SIZE}" 'BEGIN{split("B KB MB GB",u); i=1; while(b>=1024&&i<4){b/=1024;i++} printf "%.2f %s", b, u[i]}')
echo "parquet file: ${PARQUET_FILE} (${human_pq})"

# --- 2. Fresh serened for the actual benchmark -----------------------------
rm -rf "${SERENED_DATA_DIR}" ${RESULTS_DIR}/types_genparquet_data
echo "starting bench serened with data dir ${SERENED_DATA_DIR}"
start_server
trap "killall -9 serened >/dev/null 2>&1 || true" EXIT

NDB_SQL_PATH=$(printf '%s' "${NATIVE_DB}" | sed "s/'/''/g")
run_setup "attach_native_db" "${BUILD_THREADS}" "
ATTACH '${NDB_SQL_PATH}' AS native_db (TYPE duckdb);
SET search_path TO public, native_db.main;
"

run_sql "create_view" "${BUILD_THREADS}" "
CREATE VIEW bench_view AS SELECT * FROM read_parquet('${PQ_SQL_PATH}');
"

run_sql "create_native_table" "${BUILD_THREADS}" "
CREATE TABLE native_db.main.bench_native AS
SELECT * FROM read_parquet('${PQ_SQL_PATH}');
"

run_setup "checkpoint_native_db" "${BUILD_THREADS}" "CHECKPOINT native_db;"

run_sql "create_index" "${BUILD_THREADS}" "
CREATE INDEX bench_idx ON bench_view USING inverted()
INCLUDE (
  i8, i16, i32, i64, f32, f64, s, bool_col,
  arr_i32, lst_i32, struct_basic, map_i32, lst_struct, deep
);
"

# --- 3. Per-type benchmarks ------------------------------------------------
# Each pair runs the same aggregate against the cs index and the native
# DuckDB table.  `phase` is "cold" or "hot" -- only used as a label in
# TIMINGS so we can print both columns at the end.
bench_pair_idx() {
	local phase="$1" label="$2" expr="$3"
	run_sql "${phase}_${label}_indexed" "${SCAN_THREADS}" \
		"SELECT ${expr} FROM bench_idx;"
	run_sql "${phase}_${label}_native" "${SCAN_THREADS}" \
		"SELECT ${expr} FROM bench_native;"
}

QUERIES=(
	"count|COUNT(*)"
	"i8|SUM(i8::BIGINT)"
	"i16|SUM(i16::BIGINT)"
	"i32|SUM(i32::BIGINT)"
	"i64|SUM(i64)"
	"f32|SUM(f32::DOUBLE)"
	"f64|SUM(f64)"
	"varchar|SUM(length(s))"
	"bool|SUM(CASE WHEN bool_col THEN 1 ELSE 0 END)"
	"array|SUM(arr_i32[1] + arr_i32[2] + arr_i32[3])"
	"list|SUM(list_sum(lst_i32))"
	"struct|SUM(struct_basic.a) + SUM(length(struct_basic.b))"
	"map|SUM(list_sum(map_values(map_i32)))"
	"lstStr|SUM(list_sum(list_transform(lst_struct, p -> p.v)))"
	"deep|SUM(length(deep.name)) + SUM(list_sum(list_transform(deep.vals, p -> p.v)))"
)

# Two cold modes, picked by PERF_DROP_CACHES=1:
#
# - PERF_DROP_CACHES unset (default): "cold" means 1st-touch within the
#   same session.  Captures per-segment open + per-codec init cost (the
#   hot pass amortises both); does NOT capture OS-cache fetch cost
#   because the .cs files are still in page cache from CREATE INDEX.
#
# - PERF_DROP_CACHES=1: true cold.  Before every query we run
#     sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
#   while serened keeps running -- drop_caches=3 evicts clean
#   file-backed pages even when they're mmapped, so serened's next
#   access page-faults and re-reads from disk.  We deliberately do NOT
#   restart serened: view-backed `CREATE INDEX IF NOT EXISTS` after
#   restart appends fresh segments to the existing index name (the
#   rocksdb catalog row survives but the alias doesn't, and the engine
#   re-emits .cs blocks), which doubles the indexed data and the SUM
#   results.  No-restart drop is the only clean option until the
#   view-backed-index re-bind is fixed upstream.
#
#   sudo runs *only* the drop_caches command, not anything else; sudo
#   credentials are cached once via `sudo -v` at the start so the bench
#   runs uninterrupted afterwards.  Other files produced by this script
#   stay owned by the invoking user.

drop_os_cache_with_sudo() {
	# Flush dirty pages first so drop_caches=3 can free clean copies
	# (drop_caches never frees dirty pages).
	sync
	if ! sudo -n sh -c "echo 3 > /proc/sys/vm/drop_caches" 2>/dev/null; then
		echo "ERROR: sudo for drop_caches expired; run 'sudo -v' and retry" >&2
		return 1
	fi
}

# Mode A: same-session cold + warmup + hot
run_pass_same_session() {
	for q in "${QUERIES[@]}"; do
		local label="${q%%|*}" expr="${q#*|}"
		bench_pair_idx "cold" "${label}" "${expr}" # first-touch
		bench_pair_idx "warm" "${label}" "${expr}" # 2nd hit, ignored
		bench_pair_idx "hot" "${label}" "${expr}"  # 3rd hit, steady-state
	done
}

# Mode B: drop the OS page cache before each query (no server
# restart), then run cold + hot.
run_pass_drop_caches() {
	for q in "${QUERIES[@]}"; do
		local label="${q%%|*}" expr="${q#*|}"
		drop_os_cache_with_sudo || return 1
		bench_pair_idx "cold" "${label}" "${expr}"
		bench_pair_idx "hot" "${label}" "${expr}"
	done
}

echo
if [[ "${PERF_DROP_CACHES:-0}" == "1" ]]; then
	# Cache the sudo credential up front. Without this, sudo would
	# prompt mid-loop and stall the bench.
	echo "PERF_DROP_CACHES=1: caching sudo credential for drop_caches"
	if ! sudo -v; then
		echo "ERROR: failed to acquire sudo for drop_caches" >&2
		exit 1
	fi
	# Refresh in the background so a long bench doesn't expire the cache.
	(
		while kill -0 "$$" 2>/dev/null; do
			sudo -n true 2>/dev/null || break
			sleep 30
		done
	) &
	SUDO_REFRESH_PID=$!
	trap "kill ${SUDO_REFRESH_PID} 2>/dev/null; killall -9 serened >/dev/null 2>&1 || true" EXIT
	echo "================ TRUE-COLD / HOT PASS (drop_caches) ================"
	run_pass_drop_caches
else
	echo "================ COLD / HOT PASS (same-session) ================"
	echo "rerun with PERF_DROP_CACHES=1 for drop-OS-cache cold timings"
	run_pass_same_session
fi

# --- 4. Sizes --------------------------------------------------------------
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
	awk -v b="$1" 'BEGIN{split("B KB MB GB TB",u); i=1; while(b>=1024&&i<5){b/=1024;i++} printf "%.2f %s", b, u[i]}'
}

ndb_main=0
ndb_wal=0
[[ -f "${NATIVE_DB}" ]] && ndb_main=$(du -sb "${NATIVE_DB}" | awk '{print $1}')
[[ -f "${NATIVE_DB}.wal" ]] && ndb_wal=$(du -sb "${NATIVE_DB}.wal" | awk '{print $1}')
ndb_total=$((ndb_main + ndb_wal))
cs_total=$(sum_ext "${SERENED_DATA_DIR}" "cs")
ser_total=$(du -sb "${SERENED_DATA_DIR}" | awk '{print $1}')

# --- 5. Summary ------------------------------------------------------------
fmt_ms() {
	local v="$1"
	[[ -z "${v}" ]] && {
		printf 'n/a'
		return
	}
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
	echo "rows:          ${N}"
	echo "parquet file:  ${PARQUET_FILE} (${human_pq})"
	echo "build threads: ${BUILD_THREADS}    scan threads: ${SCAN_THREADS}"
	echo
	printf "%-10s | %s\n" "phase" "cold = 1st-touch same-session; hot = 3rd-touch steady-state"
	echo
	printf "%-10s %12s %12s %8s | %12s %12s %8s\n" \
		"query" "cs cold" "native cold" "cold x" \
		"cs hot" "native hot" "hot x"
	printf "%-10s %12s %12s %8s | %12s %12s %8s\n" \
		"----------" "------------" "------------" "--------" \
		"------------" "------------" "--------"
	for q in count i8 i16 i32 i64 f32 f64 varchar bool array list struct map \
		lstStr deep; do
		ci="${TIMINGS[cold_${q}_indexed]:-}"
		cn="${TIMINGS[cold_${q}_native]:-}"
		hi="${TIMINGS[hot_${q}_indexed]:-}"
		hn="${TIMINGS[hot_${q}_native]:-}"
		printf "%-10s %12s %12s %8s | %12s %12s %8s\n" "${q}" \
			"$(fmt_ms "${ci}")" "$(fmt_ms "${cn}")" "$(ratio "${cn}" "${ci}")" \
			"$(fmt_ms "${hi}")" "$(fmt_ms "${hn}")" "$(ratio "${hn}" "${hi}")"
	done
	echo
	printf "%-26s %12s\n" "storage" "bytes"
	printf "%-26s %12s\n" "--------------------------" "------------"
	printf "%-26s %12s (%s)\n" "parquet (source)" "${PQ_SIZE}" \
		"$(human "${PQ_SIZE}")"
	printf "%-26s %12s (%s)\n" "duckdb native (post-ckpt)" "${ndb_total}" \
		"$(human "${ndb_total}")"
	printf "%-26s %12s (%s)\n" "serened cs only (.cs)" "${cs_total}" \
		"$(human "${cs_total}")"
	printf "%-26s %12s (%s)\n" "serened data dir total" "${ser_total}" \
		"$(human "${ser_total}")"
	echo "========================================="
} | tee -a "${RUN_LOG}"

echo
echo "log: ${RUN_LOG}"
echo "server log: ${LOG}"
