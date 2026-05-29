#!/usr/bin/env bash
# Profile the writer side of types_perf: CREATE INDEX vs the duckdb-native
# CTAS, back-to-back perf record runs against a fresh serened on its own
# port. Used to chase "why is CREATE INDEX 4x CTAS when it should be ~2x".
#
# Each run is fully independent (kill+restart serened, perf record from
# scratch) so the perf.data files are directly comparable -- same parquet,
# same threads, same column shape.
#
# Inputs:
#   PERF_PARQUET_FILE  default ~/data/types_perf.parquet
#   PERF_BUILD_DIR     default ../../build_perf
#   PERF_PROFILE_PORT  default 6363
#   PERF_THREADS       default $(nproc)
#   PERF_FREQ          default 199
#   PERF_CALL_GRAPH    default fp

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"

PARQUET_FILE="${PERF_PARQUET_FILE:-${HOME}/data/types_perf.parquet}"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
DATA_DIR="${PERF_PROFILE_DATA_DIR:-${ROOT}/build_perf_types_writer_data}"
NATIVE_DB="${PERF_NATIVE_DB:-${ROOT}/build_perf_types_writer_native.duckdb}"
PORT="${PERF_PROFILE_PORT:-6363}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${RESULTS_DIR}/profile-types-writer-${STAMP}"
FREQ="${PERF_FREQ:-199}"
THREADS="${PERF_THREADS:-$(nproc)}"

PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"
PQ_SQL_PATH=$(printf '%s' "${PARQUET_FILE}" | sed "s/'/''/g")
NDB_SQL_PATH=$(printf '%s' "${NATIVE_DB}" | sed "s/'/''/g")

if [[ ! -f "${PARQUET_FILE}" ]]; then
	echo "missing ${PARQUET_FILE} -- run run_types_perf.sh first" >&2
	exit 1
fi
if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN}" >&2
	exit 1
fi

mkdir -p "${OUT_DIR}"

start_serened_under_perf() {
	local perf_data="$1" serened_log="$2"
	rm -rf "${DATA_DIR}"
	# perf wraps the whole serened lifetime so the build + setup work
	# also show up in the profile -- we want to see absolutely everything
	# during the targeted query window.
	perf record -F "${FREQ}" -g --call-graph "${PERF_CALL_GRAPH:-fp}" \
		--output "${perf_data}" -- \
		"${SERENED_BIN}" "${DATA_DIR}" \
		--server.endpoint "pgsql+tcp://0.0.0.0:${PORT}" \
		--log.foreground-tty true \
		>"${serened_log}" 2>&1 &
	PERF_PID=$!
	for _ in $(seq 1 60); do
		if psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.5
	done
	echo "serened did not come up; tail of ${serened_log}:" >&2
	tail -30 "${serened_log}" >&2
	exit 1
}

stop_serened() {
	kill -INT "${PERF_PID}" 2>/dev/null || true
	wait "${PERF_PID}" 2>/dev/null || true
}

dump_top() {
	local data="$1" out_prefix="$2"
	echo
	echo "=== top symbols (self) -- ${out_prefix} ==="
	perf report --no-children --stdio -g none --input "${data}" 2>/dev/null |
		head -40 | tee "${out_prefix}.top_symbols.txt"
	echo
	echo "=== top stacks (callee, depth-pruned) -- ${out_prefix} ==="
	perf report --stdio -g graph,1.5,callee --input "${data}" 2>/dev/null |
		head -80 | tee "${out_prefix}.top_stacks.txt"
}

kill_on_port() {
	local port="$1"
	local pid
	pid=$(lsof -t -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)
	if [[ -n "${pid}" ]]; then
		kill -9 "${pid}" 2>/dev/null || true
		sleep 1
	fi
}

# Targeted kill so we don't disturb another serened the user runs on a
# different port (e.g. the bench server on 6263).
kill_on_port "${PORT}"
rm -f "${NATIVE_DB}" "${NATIVE_DB}.wal"

# ============================================================
# Run 1: CREATE INDEX
# ============================================================
echo "=== Run 1: CREATE INDEX (perf record) ==="
PERF_DATA_INDEX="${OUT_DIR}/perf-create-index.data"
start_serened_under_perf "${PERF_DATA_INDEX}" "${OUT_DIR}/serened-index.log"

# View + index (matches run_types_perf.sh's view-backed shape).
psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
	-c "SET threads = ${THREADS};" \
	-c "CREATE OR REPLACE VIEW bench_view AS
	    SELECT * FROM read_parquet('${PQ_SQL_PATH}');" \
	2>&1 | tee "${OUT_DIR}/setup-index.log"

echo "timing CREATE INDEX ..."
psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
	-c "SET threads = ${THREADS};" \
	-c '\timing on' \
	-c "CREATE INDEX bench_idx ON bench_view USING inverted(pk) INCLUDE (
	      i8, i16, i32, i64, f32, f64, s, bool_col,
	      arr_i32, arr_f64, lst_i32, struct_basic, struct_f64,
	      map_i32, lst_struct, deep
	    );" \
	2>&1 | tee "${OUT_DIR}/create-index.log"

stop_serened
dump_top "${PERF_DATA_INDEX}" "${OUT_DIR}/create-index"

# ============================================================
# Run 2: native CTAS
# ============================================================
echo
echo "=== Run 2: CREATE TABLE native_db.bench_native AS SELECT (perf record) ==="
rm -f "${NATIVE_DB}" "${NATIVE_DB}.wal"
PERF_DATA_CTAS="${OUT_DIR}/perf-ctas.data"
start_serened_under_perf "${PERF_DATA_CTAS}" "${OUT_DIR}/serened-ctas.log"

psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
	-c "SET threads = ${THREADS};" \
	-c "ATTACH '${NDB_SQL_PATH}' AS native_db (TYPE duckdb, STORAGE_VERSION latest);" \
	-c "SET search_path TO public, native_db.main;" \
	2>&1 | tee "${OUT_DIR}/setup-ctas.log"

echo "timing CTAS ..."
psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
	-c "SET threads = ${THREADS};" \
	-c '\timing on' \
	-c "CREATE TABLE native_db.main.bench_native AS
	    SELECT * FROM read_parquet('${PQ_SQL_PATH}');" \
	-c "CHECKPOINT native_db;" \
	2>&1 | tee "${OUT_DIR}/ctas.log"

stop_serened
dump_top "${PERF_DATA_CTAS}" "${OUT_DIR}/ctas"

echo
echo "=== done ==="
echo "results: ${OUT_DIR}"
echo "  perf data: ${PERF_DATA_INDEX}, ${PERF_DATA_CTAS}"
echo "  reports:   ${OUT_DIR}/*.top_symbols.txt, ${OUT_DIR}/*.top_stacks.txt"
