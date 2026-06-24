#!/usr/bin/env bash
# Materialise small reproducible subsets of hits.parquet next to the
# original. Uses the perf serened binary on a side port so it doesn't
# disturb a running benchmark on 6262.
#
# Output:
#   ${PERF_PARQUET_DIR}/hits_10k.parquet   (10 k rows, debug iteration)
#   ${PERF_PARQUET_DIR}/hits_1pct.parquet  (~1 M rows, deterministic LIMIT)
#   ${PERF_PARQUET_DIR}/hits_10pct.parquet (~10 M rows, deterministic LIMIT)
#
# Run after scripts/perf/download_hits.sh.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"

PARQUET_DIR="${PERF_PARQUET_DIR:-${HOME}/data}"
SOURCE_PARQUET="${PERF_PARQUET_FILE:-${PARQUET_DIR}/hits.parquet}"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
PORT="${PERF_SAMPLE_PORT:-6464}"
TMP_DATA_DIR="$(mktemp -d -t serened-sample-XXXXXX)"
LOG="/tmp/${USER}-serened-sample.log"

# Default sizes -- LIMIT is deterministic and fast, since DuckDB can stop the
# parquet scan once it has emitted the requested row count.
ROWS_10K="${PERF_ROWS_10K:-10000}"
ROWS_1PCT="${PERF_ROWS_1PCT:-1000000}"
ROWS_10PCT="${PERF_ROWS_10PCT:-10000000}"

if [[ ! -f "${SOURCE_PARQUET}" ]]; then
	echo "missing ${SOURCE_PARQUET} -- run scripts/perf/download_hits.sh first" >&2
	exit 1
fi
if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN} -- build the perf binary first" >&2
	exit 1
fi

cleanup() {
	if [[ -n "${SERENED_PID:-}" ]]; then
		kill -9 "${SERENED_PID}" 2>/dev/null || true
	fi
	rm -rf "${TMP_DATA_DIR}"
}
trap cleanup EXIT

echo "starting ${SERENED_BIN} on port ${PORT} (tmp data dir ${TMP_DATA_DIR})"
"${SERENED_BIN}" "${TMP_DATA_DIR}" \
	--listen "postgres://0.0.0.0:${PORT}" \
	>"${LOG}" 2>&1 &
SERENED_PID=$!

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

# Single-quote-escape the parquet path for embedding in SQL.
SRC_SQL_PATH=$(printf '%s' "${SOURCE_PARQUET}" | sed "s/'/''/g")

sample_to() {
	local out_path="$1" rows="$2"
	local out_sql_path
	out_sql_path=$(printf '%s' "${out_path}" | sed "s/'/''/g")
	echo "writing ${out_path} (${rows} rows)..."
	psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X -c "\timing on" -c "
COPY (
  SELECT * FROM read_parquet('${SRC_SQL_PATH}') LIMIT ${rows}
) TO '${out_sql_path}' (FORMAT 'parquet');
"
	echo "  -> $(du -sh "${out_path}" | awk '{print $1}')"
}

sample_to "${PARQUET_DIR}/hits_10k.parquet" "${ROWS_10K}"
sample_to "${PARQUET_DIR}/hits_1pct.parquet" "${ROWS_1PCT}"
sample_to "${PARQUET_DIR}/hits_10pct.parquet" "${ROWS_10PCT}"

echo
echo "samples:"
ls -lh "${PARQUET_DIR}/hits_10k.parquet" \
	"${PARQUET_DIR}/hits_1pct.parquet" \
	"${PARQUET_DIR}/hits_10pct.parquet"
