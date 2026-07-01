#!/usr/bin/env bash
# perf-record the WRITE (CREATE INDEX over all-columns INCLUDE) and READ (query
# suite) hot paths of the iresearch .col columnstore, against our build_perf
# serened, and print the top hotspots. Guides the write-path optimization.
#
# build_perf is built with -fno-omit-frame-pointer, so frame-pointer call graphs
# are cheap + accurate (no dwarf unwinding needed). Requires perf; for kernel
# symbols run with kernel.perf_event_paranoid=-1 + kptr_restrict=0.
#
# Env: CS_PARQUET (~/data/hits_10pct.parquet), CS_BIN (build_perf/bin/serened),
#      CS_PORT (6264), CS_FREQ (999).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BIN="${CS_BIN:-${ROOT}/build_perf/bin/serened}"
PARQUET="${CS_PARQUET:-${HOME}/data/hits_10pct.parquet}"
PORT="${CS_PORT:-6264}"
FREQ="${CS_FREQ:-999}"
DATA="${CS_DATA:-${ROOT}/scripts/perf/results/cs_perf_data}"
OUT="${ROOT}/scripts/perf/results/perf"
CONN="postgres://postgres@localhost:${PORT}/postgres"
SLOG="/tmp/${USER}-cs-perf-serened.log"

[[ -x "$BIN" ]] || {
	echo "missing $BIN (build_perf)" >&2
	exit 1
}
[[ -f "$PARQUET" ]] || {
	echo "missing $PARQUET" >&2
	exit 1
}
mkdir -p "$OUT"
rm -rf "$DATA"
mkdir -p "$DATA"

killall -9 serened >/dev/null 2>&1 || true
sleep 1
"$BIN" "$DATA" --listen="postgres://0.0.0.0:${PORT}" >"$SLOG" 2>&1 &
PID=$!
trap 'kill -9 "$PID" >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 120); do
	psql "$CONN" -c 'SELECT 1' >/dev/null 2>&1 && break
	sleep 0.5
done

pq="$(printf '%s' "$PARQUET" | sed "s/'/''/g")"
psql "$CONN" -v ON_ERROR_STOP=1 -X \
	-c "CREATE TEXT SEARCH DICTIONARY perf_english(template='delimiter', delimiter=' ');" \
	-c "CREATE VIEW hits_view AS SELECT * FROM read_parquet('$pq');" >/dev/null
inc="$(psql "$CONN" -At -v ON_ERROR_STOP=1 -X \
	-c "SELECT string_agg('\"'||column_name||'\"', ', ') FROM (DESCRIBE hits_view);")"

echo "=== perf-record WRITE (CREATE INDEX) ==="
perf record -g --call-graph=fp -F "$FREQ" -p "$PID" -o "$OUT/write.perf.data" -- \
	psql "$CONN" -v ON_ERROR_STOP=1 -X -c '\timing on' \
	-c "CREATE INDEX hits_idx ON hits_view USING inverted(\"Title\" perf_english) INCLUDE ($inc);" 2>&1 | tail -3

declare -a Q=(
	'SELECT sum("ResolutionWidth") FROM hits_idx;'
	'SELECT "RegionID", count(*) c FROM hits_idx GROUP BY "RegionID" ORDER BY c DESC LIMIT 10;'
	'SELECT count(DISTINCT "UserID") FROM hits_idx;'
	'SELECT sum("ResolutionWidth")+sum("CounterID")+sum(length("URL")) FROM hits_idx;'
)
# warm once (page cache) so the read profile is CPU-bound, not IO-bound.
for q in "${Q[@]}"; do psql "$CONN" -X -c "SET threads = 1;" -c "$q" >/dev/null 2>&1 || true; done
echo "=== perf-record READ (query suite, hot, threads=1) ==="
perf record -g --call-graph=fp -F "$FREQ" -p "$PID" -o "$OUT/read.perf.data" -- \
	bash -c 'for i in 1 2 3; do for q in "$@"; do psql "'"$CONN"'" -X -c "SET threads = 1;" -c "$q" >/dev/null 2>&1; done; done' _ "${Q[@]}"

kill -9 "$PID" >/dev/null 2>&1 || true
trap - EXIT

for phase in write read; do
	echo
	echo "================= ${phase^^} HOTSPOTS (self%) ================="
	perf report -i "$OUT/${phase}.perf.data" --stdio --percent-limit 0.8 --sort=overhead,symbol 2>/dev/null |
		awk '/^# Overhead/{p=1} p&&/^ /{print} /Samples:/{print}' | head -40
done
echo
echo "perf.data: $OUT/{write,read}.perf.data  (perf report -i <file> -g to drill in)"
