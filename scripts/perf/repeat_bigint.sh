#!/usr/bin/env bash
# Repeat the bigint online phase N times for each binary; report mean wall+cpu.
set -euo pipefail
N="${1:-5}"
ROWS="${ROWS:-1000000}"

export PGPASSWORD=postgres
run_one() {
	local bin="$1" port="$2" label="$3"
	local data="/tmp/perf-repeat-${USER}-${label}"
	rm -rf "$data"
	mkdir -p "$data"
	"$bin" "$data" --server_endpoints="pgsql+tcp://0.0.0.0:${port}" >/dev/null 2>&1 &
	local pid=$!
	# wait ready
	for _ in $(seq 1 60); do
		psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 && break
		sleep 0.3
	done
	local psql="psql -h 127.0.0.1 -p $port -U postgres -d postgres -v ON_ERROR_STOP=1"
	# prepare (table + empty index)
	$psql <<<'DROP TABLE IF EXISTS sweep_t; CREATE TABLE sweep_t (pk INTEGER PRIMARY KEY, body BIGINT);' >/dev/null
	$psql -c 'CREATE INDEX sweep_idx ON sweep_t USING inverted(pk, body);' >/dev/null

	# read serened cpu+wall: /proc/$pid/stat fields utime,stime (in ticks)
	local clk
	clk=$(getconf CLK_TCK)
	read_cpu_ticks() {
		local rest=${1#*) }
		echo "$rest" | awk '{print $12, $13}'
	}
	local cpu_before t0 t1 cpu_after
	cpu_before=$(read_cpu_ticks "$(cat /proc/$pid/stat)")
	t0=$(date +%s.%N)
	$psql <<<"INSERT INTO sweep_t SELECT id, (1700000000000 + id::BIGINT * 1000000 + (id * 99991) % 86400000000)::BIGINT FROM range(${ROWS}) AS r(id);" >/dev/null
	t1=$(date +%s.%N)
	cpu_after=$(read_cpu_ticks "$(cat /proc/$pid/stat)")
	local wall cpu
	wall=$(awk -v t0="$t0" -v t1="$t1" 'BEGIN { printf "%.3f", t1 - t0 }')
	cpu=$(awk -v b="$cpu_before" -v a="$cpu_after" -v clk="$clk" '
    BEGIN {
      split(b, B, " "); split(a, A, " ");
      printf "%.3f", (A[1] - B[1] + A[2] - B[2]) / clk;
    }')
	echo "$wall $cpu"
	kill -9 $pid 2>/dev/null || true
	rm -rf "$data"
}

run_set() {
	local bin="$1" port="$2" label="$3"
	local sum_wall=0 sum_cpu=0 vals_wall=() vals_cpu=()
	echo "== $label ($bin) =="
	for i in $(seq 1 "$N"); do
		res=$(run_one "$bin" "$port" "$label")
		wall=$(echo "$res" | awk '{print $1}')
		cpu=$(echo "$res" | awk '{print $2}')
		echo "  run $i: wall=${wall}s cpu=${cpu}s"
		vals_wall+=("$wall")
		vals_cpu+=("$cpu")
	done
	local mw mc
	mw=$(printf '%s\n' "${vals_wall[@]}" | awk '{s+=$1} END{printf "%.3f", s/NR}')
	mc=$(printf '%s\n' "${vals_cpu[@]}" | awk '{s+=$1} END{printf "%.3f", s/NR}')
	echo "  mean wall=${mw}s cpu=${mc}s"
}

run_set "$(pwd)/../serenedb-main/build_perf/bin/serened" 6193 main
run_set "$(pwd)/build_perf/bin/serened" 6194 mine
