#!/usr/bin/env bash
# Profile the columnstore CREATE INDEX path on a single sample with perf.
#
# - Spawns serened (release perf binary by default) under `perf record -g`.
# - Creates a view + dictionary + INCLUDE-everything index on the chosen
#   parquet sample.
# - Stops serened when the build finishes; perf.data is dumped into
#   scripts/perf/results/profile-<timestamp>/.
# - Generates a focused symbol report and (if the script is available) a
#   flame graph.
#
# Defaults to the small sample so the profile finishes in seconds. Override
# with PERF_PARQUET_FILE=...
#
# Pre-reqs:
#   sudo apt install linux-tools-common linux-tools-generic
#   - perf needs `kernel.perf_event_paranoid <= 1` for unprivileged sampling:
#       sudo sysctl kernel.perf_event_paranoid=1
#     (otherwise re-run this script via sudo, but the output files end up
#      owned by root.)
#   - For flame graphs: clone https://github.com/brendangregg/FlameGraph and
#     put `stackcollapse-perf.pl` + `flamegraph.pl` on PATH.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"

PARQUET_FILE="${PERF_PARQUET_FILE:-${HOME}/data/hits_1pct.parquet}"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
DATA_DIR="${PERF_PROFILE_DATA_DIR:-${ROOT}/build_perf_profile_data}"
PORT="${PERF_PROFILE_PORT:-6363}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${RESULTS_DIR}/profile-${STAMP}"
PERF_DATA="${OUT_DIR}/perf.data"
FREQ="${PERF_FREQ:-199}"

if [[ ! -f "${PARQUET_FILE}" ]]; then
	echo "missing ${PARQUET_FILE}" >&2
	exit 1
fi
if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN} -- build the perf binary first" >&2
	exit 1
fi
if ! command -v perf >/dev/null 2>&1; then
	echo "perf not found -- install linux-tools" >&2
	exit 1
fi

mkdir -p "${OUT_DIR}"

# Fresh state.
killall -9 serened 2>/dev/null || true
sleep 1
rm -rf "${DATA_DIR}"

echo "starting ${SERENED_BIN} under perf record (freq=${FREQ})"
echo "  data:   ${DATA_DIR}"
echo "  perf:   ${PERF_DATA}"
# `--call-graph fp` walks frame pointers -- the perf binary is built with
# -fno-omit-frame-pointer (and -mno-omit-leaf-frame-pointer), so unwinding
# is reliable and far cheaper than DWARF (no per-sample stack dumps,
# smaller perf.data, less wall-clock overhead). Override with PERF_CALL_GRAPH
# if you ever build without frame pointers.
perf record -F "${FREQ}" -g --call-graph "${PERF_CALL_GRAPH:-fp}" \
	--output "${PERF_DATA}" -- \
	"${SERENED_BIN}" "${DATA_DIR}" \
	--server_endpoint "pgsql+tcp://0.0.0.0:${PORT}" \
	>"${OUT_DIR}/serened.log" 2>&1 &
PERF_PID=$!
trap "kill -INT ${PERF_PID} 2>/dev/null || true" EXIT

PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"
for _ in $(seq 1 30); do
	if psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
		break
	fi
	sleep 0.5
done
if ! psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
	echo "serened did not come up; serened.log:"
	tail -30 "${OUT_DIR}/serened.log"
	exit 1
fi

PQ_SQL_PATH=$(printf '%s' "${PARQUET_FILE}" | sed "s/'/''/g")

# PERF_DICT_TEMPLATE = delimiter (default) | text
PERF_DICT_TEMPLATE="${PERF_DICT_TEMPLATE:-delimiter}"
case "${PERF_DICT_TEMPLATE}" in
delimiter)
	DICT_SQL="CREATE TEXT SEARCH DICTIONARY perf_english(template = 'delimiter', delimiter = ' ');"
	;;
text)
	DICT_SQL="CREATE TEXT SEARCH DICTIONARY perf_english(template = 'text', locale = 'en_US.UTF-8', case = 'none', stemming = false, accent = false, frequency = true, position = true);"
	;;
*)
	echo "PERF_DICT_TEMPLATE must be 'delimiter' or 'text', got '${PERF_DICT_TEMPLATE}'" >&2
	exit 1
	;;
esac

psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
	-c "CREATE VIEW hits_view AS SELECT * FROM read_parquet('${PQ_SQL_PATH}');" \
	-c "${DICT_SQL}" \
	>"${OUT_DIR}/setup.log" 2>&1

# PERF_INDEX_MODE = include_all  (default; Title indexed + every other col INCLUDE'd)
#                 | title_only   (Title text-indexed, no INCLUDE -- isolates the
#                                 iresearch tokenization/posting-write cost so
#                                 we can subtract it from include_all to get
#                                 pure columnstore overhead)
MODE="${PERF_INDEX_MODE:-include_all}"

if [[ "${MODE}" == "title_only" ]]; then
	CREATE_INDEX_SQL="CREATE INDEX hits_idx ON hits_view USING inverted(\"Title\" perf_english);"
else
	INCLUDE_LIST=$(psql "${PSQL_CONN}" -At -v ON_ERROR_STOP=1 -X -c "
SELECT string_agg('\"' || column_name || '\"', ', ' ORDER BY column_index)
FROM duckdb_columns()
WHERE table_name = 'hits_view';
")
	CREATE_INDEX_SQL="CREATE INDEX hits_idx ON hits_view USING inverted(\"Title\" perf_english) INCLUDE (${INCLUDE_LIST});"
fi

echo "running CREATE INDEX (mode=${MODE}, timed)..."
psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
	-c '\timing on' \
	-c "${CREATE_INDEX_SQL}" |
	tee "${OUT_DIR}/create_index.log"

# Triggering SIGINT lets perf record finalise perf.data cleanly.
kill -INT "${PERF_PID}" 2>/dev/null || true
wait "${PERF_PID}" 2>/dev/null || true

echo
echo "=== top symbols (self time) ==="
perf report --no-children --stdio -g none --input "${PERF_DATA}" 2>/dev/null |
	head -40 | tee "${OUT_DIR}/top_symbols.txt"

echo
echo "=== top stacks (callee, depth-pruned) ==="
perf report --stdio -g graph,1.5,callee --input "${PERF_DATA}" 2>/dev/null |
	head -120 | tee "${OUT_DIR}/top_stacks.txt"

if command -v stackcollapse-perf.pl >/dev/null 2>&1 &&
	command -v flamegraph.pl >/dev/null 2>&1; then
	echo
	echo "=== flame graph ==="
	perf script --input "${PERF_DATA}" 2>/dev/null |
		stackcollapse-perf.pl |
		flamegraph.pl >"${OUT_DIR}/flamegraph.svg"
	echo "flamegraph: ${OUT_DIR}/flamegraph.svg"
fi

echo
echo "results: ${OUT_DIR}"
