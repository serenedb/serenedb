#!/usr/bin/env bash
# investigate_bigint_online.sh -- perf record over serened during a 1M bigint INSERT
# on a prepared inverted index. Outputs raw perf.data and a stdio report.
#
# Usage: investigate_bigint_online.sh <binary> <label> <out_dir> <port>
set -euo pipefail

BIN="$1"
LABEL="$2"
OUT="$3"
PORT="${4:-6193}"
mkdir -p "$OUT"

DATA="/tmp/perf-bigint-${USER}-${LABEL}"
rm -rf "$DATA"
mkdir -p "$DATA"

"$BIN" "$DATA" --listen="postgres://0.0.0.0:${PORT}" >"$OUT/${LABEL}_serened.log" 2>&1 &
SERVED_PID=$!
trap 'kill -9 $SERVED_PID 2>/dev/null || true; rm -rf "$DATA"' EXIT

# wait for readiness
for _ in $(seq 1 60); do
	PGPASSWORD=postgres psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 && break
	sleep 0.5
done

export PGPASSWORD=postgres
PSQL="psql -h 127.0.0.1 -p $PORT -U postgres -d postgres -v ON_ERROR_STOP=1"

# prepare: empty table + index (same as online phase prep)
$PSQL <<<'
DROP TABLE IF EXISTS sweep_t;
CREATE TABLE sweep_t (pk INTEGER PRIMARY KEY, body BIGINT);
' >/dev/null
$PSQL -c "CREATE INDEX sweep_idx ON sweep_t USING inverted(pk, body);" >/dev/null

# capture call graphs for the INSERT phase
echo "starting perf record for $LABEL pid=$SERVED_PID"
perf record -F 99 --call-graph dwarf,8192 -p "$SERVED_PID" -o "$OUT/${LABEL}.perf.data" \
	-- sleep 30 &
PERF_PID=$!

# run the same 1M bigint INSERT we use in the sweep manifest
INSERT_SQL="INSERT INTO sweep_t SELECT id, (1700000000000 + id::BIGINT * 1000000 + (id * 99991) % 86400000000)::BIGINT FROM range(1000000) AS r(id);"
echo "running INSERT (1M rows)..."
t0=$(date +%s.%N)
$PSQL <<<"$INSERT_SQL" >"$OUT/${LABEL}_insert.out" 2>"$OUT/${LABEL}_insert.err"
t1=$(date +%s.%N)
awk -v t0="$t0" -v t1="$t1" 'BEGIN { printf "%.3f\n", t1 - t0 }' >"$OUT/${LABEL}_insert.wall"
echo "INSERT wall=$(cat $OUT/${LABEL}_insert.wall)s"

# stop perf record (sleep is the workload; SIGINT it and wait)
kill -INT "$PERF_PID" 2>/dev/null || true
wait "$PERF_PID" 2>/dev/null || true

# fast function-level report (no call-graph resolution -- DWARF unwinding via
# addr2line is glacial on a 1.86GB binary; we only need the function pareto).
perf report -i "$OUT/${LABEL}.perf.data" --stdio --no-children --no-inline \
	-g none --percent-limit 0.5 >"$OUT/${LABEL}.report" 2>&1

echo "done: $OUT/${LABEL}.report"
