#!/usr/bin/env bash
# Same as investigate_read_path.sh but with --call-graph dwarf, so we can
# see WHO calls the hot leaf function. Slower record + slower report.
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

MAN="$(dirname "$0")/iresearch_sweep/cases.manifest"
IFS=$'\t' read -r _name _idx_clause _query _online _med _large <<<"$(awk -F'\t' -v c="$CASE" '$1==c {print; exit}' "$MAN")"

DATA="/tmp/perf-readcg-${USER}-${LABEL}-${CASE}"
rm -rf "$DATA"
mkdir -p "$DATA"
"$BIN" "$DATA" --listen="postgres://0.0.0.0:${PORT}" >"$OUT/${LABEL}_serened.log" 2>&1 &
SERVED_PID=$!
trap 'kill -9 $SERVED_PID 2>/dev/null || true; rm -rf "$DATA"' EXIT
for _ in $(seq 1 60); do
	psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 && break
	sleep 0.3
done
PSQL="psql -h 127.0.0.1 -p $PORT -U postgres -d postgres -v ON_ERROR_STOP=1"
SETUP_SQL=$(sed "s/@N@/${ROWS}/g" "$(dirname "$0")/iresearch_sweep/cases/${CASE}.sql")
echo "prep..."
$PSQL <<<"$SETUP_SQL" >/dev/null
$PSQL -c "CREATE INDEX sweep_idx ON sweep_t USING inverted(${_idx_clause});" >/dev/null
$PSQL -c "VACUUM (REFRESH_TABLE) sweep_t;" >/dev/null
HOT_SQL=""
for _ in $(seq 1 "$N"); do HOT_SQL+="${_query};"$'\n'; done
$PSQL <<<"${_query};" >/dev/null
echo "record..."
perf record -F 999 --call-graph dwarf,16384 -e cycles:P -p "$SERVED_PID" \
	-o "$OUT/${LABEL}.perf.data" -- \
	bash -c "$PSQL <<<\"\$HOT_SQL\" >/dev/null" 2>"$OUT/${LABEL}_perf.stderr"

# Children report shows aggregated time per caller chain.
echo "report --children..."
perf report -i "$OUT/${LABEL}.perf.data" --stdio --children --no-inline \
	--percent-limit 1.0 -g 'graph,callee,1' >"$OUT/${LABEL}.report.children" 2>&1
echo "done: $OUT/${LABEL}.report.children"
