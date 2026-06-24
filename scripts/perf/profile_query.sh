#!/usr/bin/env bash
# Profile one analytical query against the iresearch-columnstore index path
# with `perf record`. Mirrors profile_create_index.sh but the perf window is
# the timed query, not CREATE INDEX.
#
# - Spawns serened (release perf binary) on a fresh data dir.
# - Builds the view + dictionary + INCLUDE-everything index.
# - Stops perf, then attaches `perf record` to the running serened (--pid)
#   for the duration of the timed query so the profile captures only the
#   query, not the build.
# - Dumps top_symbols.txt and top_stacks.txt into
#   scripts/perf/results/profile-query-<TS>/.
#
# Tunables:
#   PERF_PARQUET_FILE   defaults to ${HOME}/data/hits_1pct.parquet
#   PERF_QUERY          one of: count_distinct (default) | groupby | topk | filter | count
#                       The five analytical shapes from run_hits_perf.sh.
#                       PERF_QUERY=custom + PERF_QUERY_SQL='<sql>' to profile your own.
#   PERF_DICT_TEMPLATE  delimiter (default) | text -- match the bench you're investigating.
#   PERF_BUILD_DIR      defaults to ${ROOT}/build_perf
#   PERF_PROFILE_PORT   defaults to 6363
#   PERF_FREQ           perf sample rate, defaults to 199
#   PERF_CALL_GRAPH     fp (default; perf binary preserves frame pointers) | dwarf
#   PERF_QUERY_THREADS  defaults to 1 (matches SCAN_THREADS in run_hits_perf.sh)
#   PERF_QUERY_REPEAT   how many times to run the timed query under perf record
#                       (default 5 -- the first run is JIT/buffer-warmup; the rest
#                        give perf enough samples to be meaningful for sub-second
#                        queries.)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"

PARQUET_FILE="${PERF_PARQUET_FILE:-${HOME}/data/hits_1pct.parquet}"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
SERENED_BIN="${BUILD_DIR}/bin/serened"
DATA_DIR="${PERF_PROFILE_DATA_DIR:-${ROOT}/build_perf_query_profile_data}"
PORT="${PERF_PROFILE_PORT:-6363}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${RESULTS_DIR}/profile-query-${STAMP}"
PERF_DATA="${OUT_DIR}/perf.data"
FREQ="${PERF_FREQ:-199}"
QUERY_KIND="${PERF_QUERY:-count_distinct}"
QUERY_THREADS="${PERF_QUERY_THREADS:-1}"
QUERY_REPEAT="${PERF_QUERY_REPEAT:-5}"
DICT_TEMPLATE="${PERF_DICT_TEMPLATE:-delimiter}"

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

case "${DICT_TEMPLATE}" in
delimiter)
	DICT_SQL="CREATE TEXT SEARCH DICTIONARY perf_english(template = 'delimiter', delimiter = ' ');"
	;;
text)
	DICT_SQL="CREATE TEXT SEARCH DICTIONARY perf_english(template = 'text', locale = 'en_US.UTF-8', case = 'none', stemming = false, accent = false, frequency = true, position = true);"
	;;
*)
	echo "PERF_DICT_TEMPLATE must be 'delimiter' or 'text', got '${DICT_TEMPLATE}'" >&2
	exit 1
	;;
esac

case "${QUERY_KIND}" in
count)
	QUERY_SQL="SELECT COUNT(*) FROM hits_idx;"
	;;
count_distinct)
	QUERY_SQL="SELECT COUNT(DISTINCT \"UserID\") FROM hits_idx;"
	;;
groupby)
	QUERY_SQL="SELECT \"RegionID\", COUNT(*) AS hits FROM hits_idx GROUP BY \"RegionID\" ORDER BY hits DESC LIMIT 10;"
	;;
topk)
	QUERY_SQL="SELECT \"UserID\", COUNT(*) AS hits FROM hits_idx GROUP BY \"UserID\" ORDER BY hits DESC LIMIT 10;"
	;;
filter)
	QUERY_SQL="SELECT COUNT(*) FROM hits_idx WHERE has_any_tokens(\"Title\", 'news');"
	;;
custom)
	if [[ -z "${PERF_QUERY_SQL:-}" ]]; then
		echo "PERF_QUERY=custom requires PERF_QUERY_SQL='...sql...'" >&2
		exit 1
	fi
	QUERY_SQL="${PERF_QUERY_SQL}"
	;;
*)
	echo "PERF_QUERY must be one of: count, count_distinct, groupby, topk, filter, custom" >&2
	exit 1
	;;
esac

mkdir -p "${OUT_DIR}"

# Fresh state.
killall -9 serened 2>/dev/null || true
sleep 1
rm -rf "${DATA_DIR}"

echo "starting ${SERENED_BIN}"
echo "  data:   ${DATA_DIR}"
echo "  perf:   ${PERF_DATA}"
echo "  query:  ${QUERY_KIND} (threads=${QUERY_THREADS}, repeats=${QUERY_REPEAT})"
echo "  dict:   ${DICT_TEMPLATE}"
"${SERENED_BIN}" "${DATA_DIR}" \
	--listen "postgres://0.0.0.0:${PORT}" \
	>"${OUT_DIR}/serened.log" 2>&1 &
SERENED_PID=$!
trap "kill -INT ${SERENED_PID} 2>/dev/null || true" EXIT

PSQL_CONN="postgres://postgres@localhost:${PORT}/postgres"
for _ in $(seq 1 30); do
	if psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
		break
	fi
	sleep 0.5
done
if ! psql "${PSQL_CONN}" -c 'SELECT 1' >/dev/null 2>&1; then
	echo "serened did not come up:"
	tail -30 "${OUT_DIR}/serened.log"
	exit 1
fi

PQ_SQL_PATH=$(printf '%s' "${PARQUET_FILE}" | sed "s/'/''/g")

# View + dict + index. INCLUDE everything so columnstore-served projections
# are available for the analytical shapes.
psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
	-c "CREATE VIEW hits_view AS SELECT * FROM read_parquet('${PQ_SQL_PATH}');" \
	-c "${DICT_SQL}" \
	>"${OUT_DIR}/setup.log" 2>&1

INCLUDE_LIST=$(psql "${PSQL_CONN}" -At -v ON_ERROR_STOP=1 -X -c "
SELECT string_agg('\"' || column_name || '\"', ', ' ORDER BY column_index)
FROM duckdb_columns()
WHERE table_name = 'hits_view';
")

echo "building index..."
psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
	-c "SET threads = $(nproc);" \
	-c "CREATE INDEX hits_idx ON hits_view USING inverted(\"Title\" perf_english) INCLUDE (${INCLUDE_LIST});" \
	>"${OUT_DIR}/build.log" 2>&1

# Warm-up run -- first execution pays per-segment .cs footer parses, the
# numbers we care about for analytic-scan profiling come from steady state.
psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
	-c "SET threads = ${QUERY_THREADS};" \
	-c "${QUERY_SQL}" \
	>"${OUT_DIR}/warmup.log" 2>&1 || true

# Attach perf record to the live serened process. --pid means we sample only
# the running server, not psql or anything else.
echo "starting perf record (freq=${FREQ})"
perf record -F "${FREQ}" -g --call-graph "${PERF_CALL_GRAPH:-fp}" \
	--output "${PERF_DATA}" \
	--pid "${SERENED_PID}" >"${OUT_DIR}/perf_record.log" 2>&1 &
PERF_PID=$!
# Tiny pause so perf is attached before psql kicks off the query.
sleep 0.2

echo "running ${QUERY_KIND} ${QUERY_REPEAT}x (timed)..."
{
	echo "-- query ${QUERY_KIND} (threads=${QUERY_THREADS}, repeat=${QUERY_REPEAT})"
	for i in $(seq 1 "${QUERY_REPEAT}"); do
		echo "-- iteration ${i}"
		psql "${PSQL_CONN}" -v ON_ERROR_STOP=1 -X \
			-c '\timing on' \
			-c "SET threads = ${QUERY_THREADS};" \
			-c "${QUERY_SQL}" 2>&1
	done
} | tee "${OUT_DIR}/query.log"

# Stop perf cleanly so it finalises perf.data.
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
