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
# Bench phases, picked by env:
#  - default (no env):     hot only -- 1 warmup + 1 timed hit per query.
#  - PERF_COLD=1:          same-session cold (1st-touch) + warmup + hot.
#  - PERF_DROP_CACHES=1:   restart + drop_caches before each query +
#                          cold + hot (PERF_DROP_CACHES wins over PERF_COLD).
#
# Regen knobs (independent):
#  - PERF_REGEN_PARQUET=1: rebuild the source parquet only. Implies a
#                          rebuild of native db + data dir to match.
#  - PERF_REGEN=1:         rebuild native db + serened data dir from the
#                          existing parquet.
# Missing artifacts auto-trigger the matching regen.
#
# Server lifecycle:
#  - PERF_EXTERNAL_SERENED=1: connect to a serened the user has already
#                             started on PERF_PORT (default 6263). The
#                             script does NOT spawn or kill serened.
#                             Incompatible with PERF_DROP_CACHES (needs
#                             a restart) and with any regen path (would
#                             surprise the user's data dir).

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
N="${PERF_ROWS:-25000000}"

if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN} -- build the perf binary first" >&2
	exit 1
fi

mkdir -p "${RESULTS_DIR}" "${PARQUET_DIR}"
RUN_LOG="${RESULTS_DIR}/types-$(date -u +%Y%m%dT%H%M%SZ).log"

declare -A TIMINGS=()

EXTERNAL_SERENED="${PERF_EXTERNAL_SERENED:-0}"
PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"

start_server() {
	if [[ "${EXTERNAL_SERENED}" == "1" ]]; then
		# Server is owned by the user; just verify it's reachable.
		for _ in $(seq 1 5); do
			if psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
				return 0
			fi
			sleep 0.5
		done
		echo "ERROR: PERF_EXTERNAL_SERENED=1 but no serened reachable on ${PORT}" >&2
		exit 1
	fi
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

BUILD_THREADS="${PERF_BUILD_THREADS:-$(nproc)}"
SCAN_THREADS="${PERF_SCAN_THREADS:-1}"
SEARCH_PATH_SQL="SET search_path TO public, native_db.main;"

extract_last_time_ms() {
	awk '/^Time: /{t=$2} END{if (t!="") printf "%s", t}' <"$1"
}

# Stream psql output live (so a long-running CREATE INDEX or a server
# crash doesn't leave you staring at a blank terminal), while also
# capturing it for the run log and the timing extractor. PIPESTATUS[0]
# is psql's exit code; on failure we surface it instead of letting
# `set -e` silently abort.
run_psql() {
	local label="$1" out_file="$2"
	shift 2
	printf '\n=== %s ===\n' "${label}" | tee -a "${RUN_LOG}"
	set +e
	psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X "$@" 2>&1 |
		tee -a "${RUN_LOG}" | tee "${out_file}"
	local rc=${PIPESTATUS[0]}
	set -e
	if [[ ${rc} -ne 0 ]]; then
		echo "ERROR: psql exited ${rc} on '${label}'" | tee -a "${RUN_LOG}" >&2
		echo "  check your serened logs for a crash / mid-query disconnect" >&2
		return ${rc}
	fi
}

run_sql() {
	local label="$1" threads="$2" sql="$3"
	local out_file
	out_file=$(mktemp)
	run_psql "${label} (threads=${threads})" "${out_file}" \
		-c "SET threads = ${threads};" \
		-c "${SEARCH_PATH_SQL}" \
		-c '\timing on' \
		-c "${sql}"
	TIMINGS["${label}"]=$(extract_last_time_ms "${out_file}")
	rm -f "${out_file}"
}

run_setup() {
	local label="$1" threads="$2" sql="$3"
	local out_file
	out_file=$(mktemp)
	run_psql "${label} (threads=${threads})" "${out_file}" \
		-c "SET threads = ${threads};" \
		-c '\timing on' \
		-c "${sql}"
	TIMINGS["${label}"]=$(extract_last_time_ms "${out_file}")
	rm -f "${out_file}"
}

PQ_SQL_PATH=$(printf '%s' "${PARQUET_FILE}" | sed "s/'/''/g")
NDB_SQL_PATH=$(printf '%s' "${NATIVE_DB}" | sed "s/'/''/g")

# Reject incompatible env combinations up-front.
if [[ "${EXTERNAL_SERENED}" == "1" ]] && [[ "${PERF_DROP_CACHES:-0}" == "1" ]]; then
	echo "ERROR: PERF_EXTERNAL_SERENED=1 is incompatible with PERF_DROP_CACHES=1" >&2
	echo "  (drop_caches needs to kill+restart serened, which would clobber the user's server)" >&2
	exit 1
fi

# Two independent regen knobs:
#   PERF_REGEN_PARQUET=1 -- rebuild the synthetic source parquet.
#                           Forces a data-dir rebuild too (the parquet
#                           contents may differ; native db + cs columns
#                           need to match).
#   PERF_REGEN=1         -- rebuild the native db + serened data dir from
#                           the existing parquet. Parquet untouched.
# Missing artifacts auto-trigger the matching regen.
NEED_PARQUET=0
NEED_DATA=0
[[ "${PERF_REGEN_PARQUET:-0}" == "1" ]] && NEED_PARQUET=1
[[ "${PERF_REGEN:-0}" == "1" ]] && NEED_DATA=1
[[ ! -f "${PARQUET_FILE}" ]] && NEED_PARQUET=1
[[ "${NEED_PARQUET}" == "1" ]] && NEED_DATA=1
{ [[ ! -f "${NATIVE_DB}" ]] || [[ ! -d "${SERENED_DATA_DIR}" ]]; } && NEED_DATA=1

if [[ "${EXTERNAL_SERENED}" == "1" ]] && [[ "${NEED_PARQUET}" == "1" ]]; then
	echo "ERROR: PERF_EXTERNAL_SERENED=1 + parquet regen is not supported." >&2
	echo "  Parquet generation needs a transient serened on PERF_PORT, which would" >&2
	echo "  conflict with your external one. Generate parquet first without" >&2
	echo "  PERF_EXTERNAL_SERENED, then reuse." >&2
	exit 1
fi
# external + data regen is supported: the script runs CREATE VIEW / CTAS /
# CREATE INDEX over psql against your serened, and does NOT `rm -rf` the
# data dir (your serened owns it -- start it with an empty data dir if
# you want a clean slate). The native_db file IS removed because it's a
# perf-script-owned artifact.

if [[ "${NEED_PARQUET}" == "1" ]]; then
	echo "regenerating parquet (${PARQUET_FILE})"
else
	echo "reusing existing parquet (${PARQUET_FILE})"
fi
if [[ "${NEED_DATA}" == "1" ]]; then
	echo "regenerating native db + serened data dir"
else
	echo "reusing existing native db (${NATIVE_DB}) and data dir (${SERENED_DATA_DIR})"
fi
echo "  (PERF_REGEN_PARQUET=1 forces parquet rebuild; PERF_REGEN=1 forces data rebuild)"

# --- 1. Parquet (if needed) ----------------------------------------------
# Generated on a transient serened that we throw away afterwards -- only
# the parquet file survives. This avoids landing the synthetic source
# data in the perf serened's rocksdb base store.
if [[ "${NEED_PARQUET}" == "1" ]]; then
	killall -9 serened >/dev/null 2>&1 || true
	sleep 1
	rm -rf "${RESULTS_DIR}/types_genparquet_data"
	rm -f "${PARQUET_FILE}"

	echo "generating ${PARQUET_FILE} (${N} rows) via temporary serened"
	"${SERENED_BIN}" "${RESULTS_DIR}/types_genparquet_data" \
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
         [(i + 0) % 50 + 0.5, (i + 1) % 50 + 0.5,
          (i + 2) % 50 + 0.5]::DOUBLE[3] AS arr_f64,
         [(i + 0) % 30, (i + 1) % 30, (i + 2) % 30, (i + 3) % 30] AS lst_i32,
         ROW(i * 2, 'b-' || (i % 100)::VARCHAR)
           ::STRUCT(a INTEGER, b VARCHAR) AS struct_basic,
         ROW((i * 0.5)::DOUBLE, 'b-' || (i % 100)::VARCHAR)
           ::STRUCT(a DOUBLE, b VARCHAR) AS struct_f64,
         MAP {'k1': i, 'k2': i * 2, 'k3': i * 3} AS map_i32,
         [ROW('p1', i)::STRUCT(k VARCHAR, v INTEGER),
          ROW('p2', i * 2)::STRUCT(k VARCHAR, v INTEGER)] AS lst_struct,
         ROW('name-' || (i % 100)::VARCHAR,
             [ROW('k1', i)::STRUCT(k VARCHAR, v INTEGER),
              ROW('k2', i * 2)::STRUCT(k VARCHAR, v INTEGER)])
           ::STRUCT(name VARCHAR, vals STRUCT(k VARCHAR, v INTEGER)[]) AS deep,
         {'a': (i * 0.5)::DOUBLE, 'b': 'b-' || (i % 100)::VARCHAR}::VARIANT
           AS variant_obj
  FROM range(${N}) t(i)
) TO '${PQ_SQL_PATH}' (FORMAT parquet);
"

	killall -9 serened >/dev/null 2>&1 || true
	sleep 1
	rm -rf "${RESULTS_DIR}/types_genparquet_data"
fi

# --- 2. Native db + serened data dir -------------------------------------
if [[ "${NEED_DATA}" == "1" ]]; then
	# With an external serened the user owns the data dir -- they decide
	# whether it's empty before launching their server. We only clean up
	# the perf-script-owned native db file.
	if [[ "${EXTERNAL_SERENED}" != "1" ]]; then
		rm -rf "${SERENED_DATA_DIR}"
	fi
	rm -f "${NATIVE_DB}" "${NATIVE_DB}.wal"
fi

if [[ "${EXTERNAL_SERENED}" == "1" ]]; then
	echo "reusing external serened on port ${PORT}"
else
	echo "starting bench serened with data dir ${SERENED_DATA_DIR}"
fi
start_server
if [[ "${EXTERNAL_SERENED}" != "1" ]]; then
	trap "killall -9 serened >/dev/null 2>&1 || true" EXIT
fi

run_setup "attach_native_db" "${BUILD_THREADS}" "
DETACH DATABASE IF EXISTS native_db;
ATTACH '${NDB_SQL_PATH}' AS native_db (TYPE duckdb, STORAGE_VERSION 'v1.5.0');
SET search_path TO public, native_db.main;
"

if [[ "${NEED_DATA}" == "1" ]]; then
	run_sql "create_view" "${BUILD_THREADS}" "
CREATE OR REPLACE VIEW bench_view AS
  SELECT * FROM read_parquet('${PQ_SQL_PATH}');
"

	run_sql "create_native_table" "${BUILD_THREADS}" "
DROP TABLE IF EXISTS native_db.main.bench_native;
CREATE TABLE native_db.main.bench_native AS
SELECT * FROM read_parquet('${PQ_SQL_PATH}');
"

	run_setup "checkpoint_native_db" "${BUILD_THREADS}" "CHECKPOINT native_db;"

	# The bench measures INCLUDE-column scan, not posting-list traversal.
	# An empty `inverted()` (PK-only) is rejected by the grammar today
	# (see docs/issue-operability-diagnostics.md "Inverted index without
	# any indexed column"), so we index the lowest-cardinality column we
	# have -- bool_col -- as a near-zero-overhead filler. Two distinct
	# values means two posting lists ~50M docs each; delta+bitpacking
	# makes them tiny vs indexing a unique-per-row column like pk
	# (which would build 100M length-1 posting lists and dominate
	# CREATE INDEX time).
	# bool_col is dual-purpose: indexed AND in INCLUDE. The bench queries
	# (bool / boolCast / boolFilt) read it via cs, so it has to be an
	# INCLUDE column; the inverted-index side carries it just to satisfy
	# the "at least one indexed column" requirement.
	run_sql "create_index" "${BUILD_THREADS}" "
DROP INDEX IF EXISTS bench_idx;
CREATE INDEX bench_idx ON bench_view USING inverted(bool_col)
INCLUDE (
  i8, i16, i32, i64, f32, f64, s, bool_col,
  arr_i32, arr_f64, lst_i32, struct_basic, struct_f64,
  map_i32, lst_struct, deep, variant_obj
);
"
fi

PQ_SIZE=$(du -sb "${PARQUET_FILE}" | awk '{print $1}')
human_pq=$(awk -v b="${PQ_SIZE}" 'BEGIN{split("B KB MB GB",u); i=1; while(b>=1024&&i<4){b/=1024;i++} printf "%.2f %s", b, u[i]}')
echo "parquet file: ${PARQUET_FILE} (${human_pq})"

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
	"boolCast|SUM(bool_col::INTEGER)"
	"boolFilt|COUNT(*) FILTER (WHERE bool_col)"
	"array|SUM(arr_i32[1] + arr_i32[2] + arr_i32[3])"
	"arrayF|SUM(arr_f64[1] + arr_f64[2] + arr_f64[3])"
	"list|SUM(list_sum(lst_i32))"
	"struct|SUM(struct_basic.a) + SUM(length(struct_basic.b))"
	"structF|SUM(struct_f64.a) + SUM(length(struct_f64.b))"
	"map|SUM(list_sum(map_values(map_i32)))"
	"lstStr|SUM(list_sum(list_transform(lst_struct, p -> p.v)))"
	"deep|SUM(length(deep.name)) + SUM(list_sum(list_transform(deep.vals, p -> p.v)))"
	"variant|SUM(variant_obj.a::DOUBLE) + SUM(1.0 / length(variant_obj.b::VARCHAR))"
)

# Three timing modes, picked by env:
#
# - default (no env): hot only.  1 warmup hit + 1 timed hit per query
#   per side.  Steady-state warm.  Fastest run.
#
# - PERF_COLD=1: same-session cold + hot.  "cold" means 1st-touch within
#   the same session (per-segment open + per-codec init cost).  Does NOT
#   capture OS-cache fetch cost because the .cs files are still in page
#   cache from CREATE INDEX, AND DuckDB's BufferManager keeps the native
#   side warm across the drop.
#
# - PERF_DROP_CACHES=1: true cold + hot.  Before each query we kill
#   serened, drop the OS page cache, restart, and re-attach native_db.
#   The restart drops both serened's BlockHandles AND DuckDB's
#   BufferManager state for the native side, so both sides re-read from
#   disk on the first query.  The view-backed `bench_idx` alias is
#   reloaded by serened on startup (fixed in #662) so no CREATE INDEX is
#   needed and no data duplication occurs.
#
#   sudo runs *only* the drop_caches command.  Credentials are cached
#   once via `sudo -v` at the start so the bench runs uninterrupted.
#   Other files this script produces stay owned by the invoking user.
#
# PERF_DROP_CACHES=1 takes precedence over PERF_COLD=1.

drop_os_cache_with_sudo() {
	# Flush dirty pages first so drop_caches=3 can free clean copies
	# (drop_caches never frees dirty pages).
	sync
	if ! sudo -n sh -c "echo 3 > /proc/sys/vm/drop_caches" 2>/dev/null; then
		echo "ERROR: sudo for drop_caches expired; run 'sudo -v' and retry" >&2
		return 1
	fi
}

reattach_native_db() {
	run_setup "cold_reattach_native_db" "${BUILD_THREADS}" "
ATTACH IF NOT EXISTS '${NDB_SQL_PATH}' AS native_db (TYPE duckdb, STORAGE_VERSION 'v1.5.0');
SET search_path TO public, native_db.main;
"
}

cycle_cold() {
	# Server has to exit so its .cs mmaps release the pages before
	# drop_caches can free them.
	killall -9 serened >/dev/null 2>&1 || true
	for _ in $(seq 1 30); do
		pgrep -f "${SERENED_BIN}" >/dev/null || break
		sleep 0.2
	done
	drop_os_cache_with_sudo || return 1
	start_server
	reattach_native_db
}

# Default mode: hot only.  1 warmup hit + 1 timed hit.
run_pass_hot_only() {
	for q in "${QUERIES[@]}"; do
		local label="${q%%|*}" expr="${q#*|}"
		bench_pair_idx "warmup" "${label}" "${expr}" # ignored
		bench_pair_idx "hot" "${label}" "${expr}"    # timed
	done
}

# PERF_COLD=1: same-session cold + warmup + hot.
run_pass_same_session() {
	for q in "${QUERIES[@]}"; do
		local label="${q%%|*}" expr="${q#*|}"
		bench_pair_idx "cold" "${label}" "${expr}" # first-touch
		bench_pair_idx "warm" "${label}" "${expr}" # 2nd hit, ignored
		bench_pair_idx "hot" "${label}" "${expr}"  # 3rd hit, steady-state
	done
}

# PERF_DROP_CACHES=1: restart-and-drop-OS-cache before each query, then
# cold + hot.  Each cycle: kill serened -> drop_caches -> restart ->
# reattach native.  Clears DuckDB BufferManager too so native isn't
# unfairly warm.
run_pass_drop_caches() {
	for q in "${QUERIES[@]}"; do
		local label="${q%%|*}" expr="${q#*|}"
		cycle_cold || {
			echo "ERROR: drop_caches cycle failed" >&2
			return 1
		}
		bench_pair_idx "cold" "${label}" "${expr}"
		bench_pair_idx "hot" "${label}" "${expr}"
	done
}

# Dispatch the bench pass.  PERF_DROP_CACHES wins over PERF_COLD.
HAS_COLD=0
echo
if [[ "${PERF_DROP_CACHES:-0}" == "1" ]]; then
	HAS_COLD=1
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
	echo "================ TRUE-COLD / HOT PASS (restart + drop_caches) ================"
	run_pass_drop_caches
elif [[ "${PERF_COLD:-0}" == "1" ]]; then
	HAS_COLD=1
	echo "================ COLD / HOT PASS (same-session) ================"
	echo "rerun with PERF_DROP_CACHES=1 for restart+drop-OS-cache cold timings"
	run_pass_same_session
else
	echo "================ HOT PASS ================"
	echo "rerun with PERF_COLD=1 for same-session cold timings,"
	echo "or PERF_DROP_CACHES=1 for restart+drop-OS-cache cold timings"
	run_pass_hot_only
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
	if [[ "${HAS_COLD}" == "1" ]]; then
		printf "%-10s | %s\n" "phase" \
			"cold = 1st-touch same-session; hot = 3rd-touch steady-state"
		echo
		printf "%-10s %12s %12s %8s | %12s %12s %8s\n" \
			"query" "cs cold" "native cold" "cold x" \
			"cs hot" "native hot" "hot x"
		printf "%-10s %12s %12s %8s | %12s %12s %8s\n" \
			"----------" "------------" "------------" "--------" \
			"------------" "------------" "--------"
		for q in count i8 i16 i32 i64 f32 f64 varchar bool boolCast boolFilt \
			array arrayF list struct structF map lstStr deep; do
			ci="${TIMINGS[cold_${q}_indexed]:-}"
			cn="${TIMINGS[cold_${q}_native]:-}"
			hi="${TIMINGS[hot_${q}_indexed]:-}"
			hn="${TIMINGS[hot_${q}_native]:-}"
			printf "%-10s %12s %12s %8s | %12s %12s %8s\n" "${q}" \
				"$(fmt_ms "${ci}")" "$(fmt_ms "${cn}")" \
				"$(ratio "${cn}" "${ci}")" \
				"$(fmt_ms "${hi}")" "$(fmt_ms "${hn}")" \
				"$(ratio "${hn}" "${hi}")"
		done
	else
		printf "%-10s | %s\n" "phase" "hot = warmup + timed hit"
		echo
		printf "%-10s %12s %12s %8s\n" \
			"query" "cs hot" "native hot" "hot x"
		printf "%-10s %12s %12s %8s\n" \
			"----------" "------------" "------------" "--------"
		for q in count i8 i16 i32 i64 f32 f64 varchar bool boolCast boolFilt \
			array arrayF list struct structF map lstStr deep; do
			hi="${TIMINGS[hot_${q}_indexed]:-}"
			hn="${TIMINGS[hot_${q}_native]:-}"
			printf "%-10s %12s %12s %8s\n" "${q}" \
				"$(fmt_ms "${hi}")" "$(fmt_ms "${hn}")" \
				"$(ratio "${hn}" "${hi}")"
		done
	fi
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
