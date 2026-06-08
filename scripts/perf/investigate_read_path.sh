#!/usr/bin/env bash
# investigate_read_path.sh -- perf record over serened while it runs N hot
# queries against a prepared inverted index for one of the regressed cases.
#
# Usage: investigate_read_path.sh <CASE> <binary> <label> <out_dir> <port> [N=1000] [ROWS=1000000]
set -euo pipefail

CASE="$1"
BIN="$2"
LABEL="$3"
OUT="$4"
PORT="${5:-6193}"
N="${6:-1000}"
ROWS="${ROWS:-1000000}"
mkdir -p "$OUT"

export PGPASSWORD=postgres

# Look up the case
MAN="$(dirname "$0")/iresearch_sweep/cases.manifest"
IFS=$'\t' read -r _name _idx_clause _query _online _med _large <<<"$(awk -F'\t' -v c="$CASE" '$1==c {print; exit}' "$MAN")"
[[ -n "$_name" ]] || {
	echo "case $CASE not in manifest" >&2
	exit 2
}

DATA="/tmp/perf-read-${USER}-${LABEL}-${CASE}"
rm -rf "$DATA"
mkdir -p "$DATA"
"$BIN" "$DATA" --server_endpoints="pgsql+tcp://0.0.0.0:${PORT}" >"$OUT/${LABEL}_serened.log" 2>&1 &
SERVED_PID=$!
trap 'kill -9 $SERVED_PID 2>/dev/null || true; rm -rf "$DATA"' EXIT

for _ in $(seq 1 60); do
	psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 && break
	sleep 0.3
done

PSQL="psql -h 127.0.0.1 -p $PORT -U postgres -d postgres -v ON_ERROR_STOP=1"

# Prep: same as sweep's setup + backfill phases
SETUP_SQL=$(sed "s/@N@/${ROWS}/g" "$(dirname "$0")/iresearch_sweep/cases/${CASE}.sql")
echo "prepping $CASE/${ROWS}..."
$PSQL <<<"$SETUP_SQL" >/dev/null
$PSQL -c "CREATE INDEX sweep_idx ON sweep_t USING inverted(${_idx_clause});" >/dev/null
$PSQL -c "VACUUM (REFRESH_TABLE) sweep_t;" >/dev/null

# Build the multi-query script: N copies of the query
HOT_SQL=""
for _ in $(seq 1 "$N"); do HOT_SQL+="${_query};"$'\n'; done

# Warm up so the first call after CREATE INDEX doesn't pull in cold pages
$PSQL <<<"${_query};" >/dev/null

# perf record at high freq during the multi-query batch. -g none keeps records
# small/fast; we just need a flat function pareto.
echo "starting perf record ($N queries)..."
perf record -F 1999 -e cycles:P -p "$SERVED_PID" -o "$OUT/${LABEL}.perf.data" -- \
	bash -c "$PSQL <<<\"\$HOT_SQL\" >/dev/null" 2>"$OUT/${LABEL}_perf.stderr"

# Function-level pareto, no symbol unwinding (fastest)
perf report -i "$OUT/${LABEL}.perf.data" --stdio --no-children --no-inline \
	-g none --percent-limit 0.3 >"$OUT/${LABEL}.report" 2>&1

echo "done: $OUT/${LABEL}.report"
