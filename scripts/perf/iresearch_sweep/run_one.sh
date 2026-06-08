#!/usr/bin/env bash
# run_one.sh -- runs all phases for a (binary, case, size) triple.
#
# Usage: run_one.sh <binary_path> <case> <rows> <out_dir> <port>
#
# Measurement strategy: read /proc/<serened-pid>/stat before/after each phase
# to capture wall-clock + serened's user+sys CPU time + maxRSS. Avoids the
# perf-stat-SIGINT fragility that doesn't flush counters reliably.
#
# Phases:
#   1. setup     -- populate the table with N rows from the case's setup SQL
#   2. backfill  -- CREATE INDEX over the populated table
#   3. compact   -- VACUUM (REFRESH_TABLE) the table to force segment merge
#   4. hot       -- run the query 10x with warm caches
#   5. cold_open -- vmtouch -e the data dir then run the query once
#                 (covers reader-open + cold-fetch cost)
#   6. cold_read -- repeat after another vmtouch -e (steady-state cold)
#   7. online    -- drop+recreate with the index in place, then INSERT N rows
#
# Per-phase output:
#   <out>/<binary>_<case>_<rows>_<phase>.{wall,cpu,rss,out,err}
set -euo pipefail

BIN="$1"
CASE="$2"
ROWS="$3"
OUT="$4"
PORT="${5:-6161}"
BIN_LABEL="${BIN_LABEL:-$(basename "$(realpath "$(dirname "$BIN")/../..")")}"
LABEL="${BIN_LABEL}_${CASE}_${ROWS}"
DATA="/tmp/perf-sweep-${USER}-${BIN_LABEL}-${CASE}-${ROWS}"
mkdir -p "$OUT"

# --- manifest lookup (case -> DDL clause, query, online-insert template) ---
MAN="$(dirname "$0")/cases.manifest"
IFS=$'\t' read -r _name _idx_clause _query _online _med _large <<<"$(awk -F'\t' -v c="$CASE" '$1==c {print; exit}' "$MAN")"
[[ -n "$_name" ]] || {
	echo "case $CASE not in manifest" >&2
	exit 2
}

# --- launch serened ---
rm -rf "$DATA"
mkdir -p "$DATA"
"$BIN" "$DATA" --server_endpoints="pgsql+tcp://0.0.0.0:${PORT}" \
	>"$OUT/${LABEL}_serened.log" 2>&1 &
SERVED_PID=$!
trap 'kill -9 $SERVED_PID 2>/dev/null || true; rm -rf "$DATA"' EXIT
# Wait for ready
ready=0
for _ in $(seq 1 60); do
	if PGPASSWORD=postgres psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1; then
		ready=1
		break
	fi
	sleep 0.5
done
[[ $ready -eq 1 ]] || {
	echo "$LABEL :: serened never became ready" >&2
	exit 3
}

PSQL="PGPASSWORD=postgres psql -h 127.0.0.1 -p $PORT -U postgres -d postgres -v ON_ERROR_STOP=1"
CLK=$(getconf CLK_TCK) # usually 100

# Read utime+stime ticks from /proc/<pid>/stat (fields 14+15).
read_cpu_ticks() {
	local stat
	stat=$(cat "/proc/$SERVED_PID/stat" 2>/dev/null) || {
		echo "0 0"
		return
	}
	# /proc/<pid>/stat: pid (comm) state ppid ... utime(14) stime(15) ...
	# comm may contain spaces and parens -- strip parenthesised section first.
	local rest=${stat#*) }
	# rest fields: state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt utime stime ...
	# so utime is field 12, stime is field 13 in rest.
	echo "$rest" | awk '{print $12, $13}'
}

# Read VmHWM in kB.
read_rss_hwm() {
	awk '/^VmHWM:/ {print $2}' "/proc/$SERVED_PID/status" 2>/dev/null || echo 0
}

# --- helpers ---
run_phase() {
	local phase="$1"
	shift
	local out_prefix="$OUT/${LABEL}_${phase}"
	echo ">>> $LABEL :: $phase"
	local cpu_before cpu_after t0 t1
	cpu_before=$(read_cpu_ticks)
	t0=$(date +%s.%N)
	if ! eval "$*" >"${out_prefix}.out" 2>"${out_prefix}.err"; then
		echo "$LABEL :: $phase FAILED -- see ${out_prefix}.err" >&2
		return 1
	fi
	t1=$(date +%s.%N)
	cpu_after=$(read_cpu_ticks)
	awk -v t0="$t0" -v t1="$t1" 'BEGIN { printf "%.6f\n", t1 - t0 }' >"${out_prefix}.wall"
	# CPU delta in seconds
	awk -v b="$cpu_before" -v a="$cpu_after" -v clk="$CLK" '
    BEGIN {
      split(b, B, " "); split(a, A, " ");
      ut = (A[1] - B[1]) / clk; st = (A[2] - B[2]) / clk;
      printf "%.6f %.6f %.6f\n", ut, st, ut + st;
    }' >"${out_prefix}.cpu"
	read_rss_hwm >"${out_prefix}.rss"
}

# --- phase 1: setup (populate table from case SQL, no index yet) ---
SETUP_SQL=$(sed "s/@N@/${ROWS}/g" "$(dirname "$0")/cases/${CASE}.sql")
run_phase setup "$PSQL <<<\"$SETUP_SQL\""

# --- phase 2: backfill (CREATE INDEX over populated table) ---
BACKFILL_SQL="CREATE INDEX sweep_idx ON sweep_t USING inverted(${_idx_clause});"
run_phase backfill "$PSQL <<<\"\$BACKFILL_SQL\""

# --- phase 3: compaction (force segment merge via REFRESH_TABLE) ---
COMPACT_SQL='VACUUM (REFRESH_TABLE) sweep_t;'
run_phase compact "$PSQL <<<\"\$COMPACT_SQL\""

# --- phase 4: hot read (10x query, warm cache) ---
HOT_SQL=""
for _ in $(seq 1 10); do HOT_SQL+="${_query};"$'\n'; done
run_phase hot "$PSQL <<<\"\$HOT_SQL\""

# --- phase 5: cold_open (vmtouch -e then one query -- covers reader-open) ---
vmtouch -e "$DATA" >/dev/null 2>&1 || true
COLD_SQL="${_query};"
run_phase cold_open "$PSQL <<<\"\$COLD_SQL\""

# --- phase 6: cold_read (vmtouch -e then second query, post-open) ---
vmtouch -e "$DATA" >/dev/null 2>&1 || true
run_phase cold_read "$PSQL <<<\"\$COLD_SQL\""

# --- phase 7: online insert (drop, recreate empty + index, then insert N) ---
eval "$PSQL <<<'DROP INDEX IF EXISTS sweep_idx;'" >/dev/null
eval "$PSQL <<<'TRUNCATE sweep_t;'" >/dev/null
PREP_SQL="CREATE INDEX sweep_idx ON sweep_t USING inverted(${_idx_clause});"
eval "$PSQL <<<\"\$PREP_SQL\"" >/dev/null
ONLINE_SQL=$(echo "$_online" | sed "s/@START@/0/g; s/@COUNT@/${ROWS}/g")
ONLINE_SQL="${ONLINE_SQL};"
run_phase online "$PSQL <<<\"\$ONLINE_SQL\""

# --- emit a simple CSV row per phase ---
{
	echo "binary,case,rows,phase,wall_seconds,cpu_user,cpu_sys,cpu_total,rss_kb"
	for phase in setup backfill compact hot cold_open cold_read online; do
		wall=$(cat "$OUT/${LABEL}_${phase}.wall" 2>/dev/null || echo "")
		cpu=$(cat "$OUT/${LABEL}_${phase}.cpu" 2>/dev/null || echo "0 0 0")
		rss=$(cat "$OUT/${LABEL}_${phase}.rss" 2>/dev/null || echo 0)
		cpu_user=$(echo "$cpu" | awk '{print $1}')
		cpu_sys=$(echo "$cpu" | awk '{print $2}')
		cpu_total=$(echo "$cpu" | awk '{print $3}')
		echo "${BIN_LABEL},${CASE},${ROWS},${phase},${wall},${cpu_user},${cpu_sys},${cpu_total},${rss}"
	done
} >"$OUT/${LABEL}.csv"

echo "<<< $LABEL done"
