#!/usr/bin/env bash
# Repeat read-path phases (cold_open, cold_read, hot, compact) per case N times
# against fresh serened processes. Each iteration uses a fresh data dir so cold
# = truly cold. Reports min/median/mean wall and serened CPU for both binaries.
#
# Usage: repeat_read_path.sh <CASE> [N=10]
set -euo pipefail

CASE="${1:?case name required}"
N="${2:-10}"
ROWS="${ROWS:-1000000}"

export PGPASSWORD=postgres
MAN="$(dirname "$0")/iresearch_sweep/cases.manifest"
IFS=$'\t' read -r _name _idx_clause _query _online _med _large <<<"$(awk -F'\t' -v c="$CASE" '$1==c {print; exit}' "$MAN")"
[[ -n "$_name" ]] || {
	echo "case $CASE not in manifest" >&2
	exit 2
}

CLK=$(getconf CLK_TCK)

run_one() {
	local bin="$1" port="$2" label="$3"
	local data="/tmp/perf-readrepeat-${USER}-${label}-${CASE}"
	rm -rf "$data"
	mkdir -p "$data"
	"$bin" "$data" --server_endpoints="pgsql+tcp://0.0.0.0:${port}" >/dev/null 2>&1 &
	local pid=$!
	for _ in $(seq 1 60); do
		psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 && break
		sleep 0.3
	done

	local psql="psql -h 127.0.0.1 -p $port -U postgres -d postgres -v ON_ERROR_STOP=1"

	# populate + index (same prep as sweep's backfill phase)
	local sql
	sql=$(sed "s/@N@/${ROWS}/g" "$(dirname "$0")/iresearch_sweep/cases/${CASE}.sql")
	$psql <<<"$sql" >/dev/null
	$psql -c "CREATE INDEX sweep_idx ON sweep_t USING inverted(${_idx_clause});" >/dev/null
	$psql -c "VACUUM (REFRESH_TABLE) sweep_t;" >/dev/null

	read_cpu_ticks() {
		local rest=${1#*) }
		echo "$rest" | awk '{print $12, $13}'
	}
	one_phase() {
		local name="$1" cmd="$2"
		vmtouch -e "$data" >/dev/null 2>&1 || true
		local cpu_before t0 t1 cpu_after wall cpu
		cpu_before=$(read_cpu_ticks "$(cat /proc/$pid/stat)")
		t0=$(date +%s.%N)
		eval "$cmd" >/dev/null
		t1=$(date +%s.%N)
		cpu_after=$(read_cpu_ticks "$(cat /proc/$pid/stat)")
		wall=$(awk -v t0="$t0" -v t1="$t1" 'BEGIN { printf "%.4f", t1 - t0 }')
		cpu=$(awk -v b="$cpu_before" -v a="$cpu_after" -v clk="$CLK" '
      BEGIN { split(b, B, " "); split(a, A, " ");
        printf "%.4f", (A[1] - B[1] + A[2] - B[2]) / clk; }')
		echo "$name $wall $cpu"
	}

	# cold_open: vmtouch -e then 1x query
	one_phase cold_open "$psql -c \"${_query};\""
	# cold_read: vmtouch -e again then 1x query (reader already opened)
	one_phase cold_read "$psql -c \"${_query};\""
	# hot: 10x query without vmtouch -e (warm cache)
	local hot_sql=""
	for _ in $(seq 1 10); do hot_sql+="${_query};"$'\n'; done
	local cpu_before t0 t1 cpu_after wall cpu
	cpu_before=$(read_cpu_ticks "$(cat /proc/$pid/stat)")
	t0=$(date +%s.%N)
	$psql <<<"$hot_sql" >/dev/null
	t1=$(date +%s.%N)
	cpu_after=$(read_cpu_ticks "$(cat /proc/$pid/stat)")
	wall=$(awk -v t0="$t0" -v t1="$t1" 'BEGIN { printf "%.4f", t1 - t0 }')
	cpu=$(awk -v b="$cpu_before" -v a="$cpu_after" -v clk="$CLK" '
    BEGIN { split(b, B, " "); split(a, A, " ");
      printf "%.4f", (A[1] - B[1] + A[2] - B[2]) / clk; }')
	echo "hot $wall $cpu"

	kill -9 $pid 2>/dev/null || true
	rm -rf "$data"
}

stats() {
	local label="$1" phase="$2"
	shift 2
	python3 -c "
import sys, statistics
vals = [float(x) for x in '''$*'''.split() if x]
print(f'    {sys.argv[1]:<10s} n={len(vals)} min={min(vals):.4f} median={statistics.median(vals):.4f} mean={statistics.mean(vals):.4f}')
" "$phase"
}

run_all() {
	local bin="$1" port="$2" label="$3"
	echo "== $label =="
	declare -A wall_phase cpu_phase
	for phase in cold_open cold_read hot; do
		wall_phase[$phase]=""
		cpu_phase[$phase]=""
	done
	for i in $(seq 1 "$N"); do
		while read -r phase wall cpu; do
			wall_phase[$phase]="${wall_phase[$phase]} $wall"
			cpu_phase[$phase]="${cpu_phase[$phase]} $cpu"
		done < <(run_one "$bin" "$port" "$label")
		echo "  run $i done"
	done
	for phase in cold_open cold_read hot; do
		echo "  $phase wall:"
		stats "$label" "wall " ${wall_phase[$phase]}
		echo "  $phase  cpu:"
		stats "$label" "cpu  " ${cpu_phase[$phase]}
	done
}

run_all "$(pwd)/../serenedb-main/build_perf/bin/serened" 6193 main
run_all "$(pwd)/build_perf/bin/serened" 6194 mine
