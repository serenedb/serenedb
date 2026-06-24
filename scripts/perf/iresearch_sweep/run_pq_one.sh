#!/usr/bin/env bash
# run_pq_one.sh -- like run_one.sh, but the dataset is materialised once into
# parquet and the indexed object is a VIEW over `read_parquet(...)`. Removes
# the RocksDB WAL / PointLockManager path (~70% of CPU in the previous
# online-INSERT profiles) so the perf signal is iresearch-only.
#
# Usage: run_pq_one.sh <binary> <case> <rows> <out_dir> <port>
#
# Phases:
#   1. setup     -- generate the dataset SQL into a regular table then COPY to
#                  parquet at /tmp/perf-pq-<user>-<bin>-<case>-<rows>.parquet
#                  (so the read side is purely DuckDB parquet, no RocksDB).
#   2. dict      -- create the case's text-search dictionary (no-op for non-text
#                  cases).
#   3. view      -- drop+create the view backed by read_parquet().
#   4. backfill  -- CREATE INDEX ON view USING inverted(<clause>). All
#                  iresearch sink + idx writer + cs writer cost lives here.
#
# Only `backfill` is timed; setup / dict / view are prep.
set -euo pipefail

BIN="$1"
CASE="$2"
ROWS="$3"
OUT="$4"
PORT="${5:-6161}"
BIN_LABEL="${BIN_LABEL:-$(basename "$(realpath "$(dirname "$BIN")/../..")")}"
LABEL="${BIN_LABEL}_${CASE}_${ROWS}"
DATA="/tmp/perf-pq-${USER}-${BIN_LABEL}-${CASE}-${ROWS}"
PARQUET="/tmp/perf-pq-${USER}-${CASE}-${ROWS}.parquet"
mkdir -p "$OUT"

MAN="$(dirname "$0")/cases.manifest"
IFS=$'\t' read -r _name _idx_clause _query _online _med _large <<<"$(awk -F'\t' -v c="$CASE" '$1==c {print; exit}' "$MAN")"
[[ -n "$_name" ]] || {
	echo "case $CASE not in manifest" >&2
	exit 2
}

rm -rf "$DATA"
mkdir -p "$DATA"
"$BIN" "$DATA" --listen="postgres://0.0.0.0:${PORT}" \
	>"$OUT/${LABEL}_serened.log" 2>&1 &
SERVED_PID=$!
trap 'kill -9 $SERVED_PID 2>/dev/null || true; rm -rf "$DATA"' EXIT
for _ in $(seq 1 60); do
	PGPASSWORD=postgres psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 && break
	sleep 0.3
done

PSQL="PGPASSWORD=postgres psql -h 127.0.0.1 -p $PORT -U postgres -d postgres -v ON_ERROR_STOP=1"
CLK=$(getconf CLK_TCK)

read_cpu_ticks() {
	local stat
	stat=$(cat "/proc/$SERVED_PID/stat" 2>/dev/null) || {
		echo "0 0"
		return
	}
	local rest=${stat#*) }
	echo "$rest" | awk '{print $12, $13}'
}

run_phase() {
	local phase="$1"
	shift
	local out_prefix="$OUT/${LABEL}_${phase}"
	local cpu_before t0 t1 cpu_after
	cpu_before=$(read_cpu_ticks)
	t0=$(date +%s.%N)
	if ! eval "$*" >"${out_prefix}.out" 2>"${out_prefix}.err"; then
		echo "$LABEL :: $phase FAILED -- see ${out_prefix}.err" >&2
		return 1
	fi
	t1=$(date +%s.%N)
	cpu_after=$(read_cpu_ticks)
	awk -v t0="$t0" -v t1="$t1" 'BEGIN { printf "%.6f\n", t1 - t0 }' >"${out_prefix}.wall"
	awk -v b="$cpu_before" -v a="$cpu_after" -v clk="$CLK" '
    BEGIN { split(b, B, " "); split(a, A, " ");
      ut = (A[1] - B[1]) / clk; st = (A[2] - B[2]) / clk;
      printf "%.6f %.6f %.6f\n", ut, st, ut + st; }' >"${out_prefix}.cpu"
}

# === setup: load data into a native DuckDB table, dump to parquet ===
# Reuse the case SQL but rename the table to avoid colliding with our view.
SETUP_SQL=$(sed "s/@N@/${ROWS}/g; s/sweep_t/sweep_src/g; s/sweep_idx/sweep_src_idx/g" \
	"$(dirname "$0")/cases/${CASE}.sql")
run_phase setup "$PSQL <<<\"$SETUP_SQL\""

# Always (re)materialise parquet. The catalog has no RocksDB-backed
# transaction state on a per-row basis after COPY ; subsequent reads go
# through duckdb's read_parquet directly.
rm -f "$PARQUET"
COPY_SQL="COPY sweep_src TO '$PARQUET' (FORMAT PARQUET);"
run_phase copy "$PSQL <<<\"$COPY_SQL\""

# === view backed by parquet ===
VIEW_SQL="DROP VIEW IF EXISTS sweep_t; CREATE VIEW sweep_t AS SELECT * FROM read_parquet('$PARQUET');"
run_phase view "$PSQL <<<\"$VIEW_SQL\""

# === backfill: CREATE INDEX over the view ===
BACKFILL_SQL="CREATE INDEX sweep_idx ON sweep_t USING inverted(${_idx_clause});"
run_phase backfill "$PSQL <<<\"\$BACKFILL_SQL\""

# emit CSV (only phases we want to compare across binaries)
{
	echo "binary,case,rows,phase,wall_seconds,cpu_user,cpu_sys,cpu_total"
	for phase in setup copy view backfill; do
		wall=$(cat "$OUT/${LABEL}_${phase}.wall" 2>/dev/null || echo "")
		cpu=$(cat "$OUT/${LABEL}_${phase}.cpu" 2>/dev/null || echo "0 0 0")
		cpu_user=$(echo "$cpu" | awk '{print $1}')
		cpu_sys=$(echo "$cpu" | awk '{print $2}')
		cpu_total=$(echo "$cpu" | awk '{print $3}')
		echo "${BIN_LABEL},${CASE},${ROWS},${phase},${wall},${cpu_user},${cpu_sys},${cpu_total}"
	done
} >"$OUT/${LABEL}.csv"
echo "<<< $LABEL done"
