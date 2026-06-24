#!/usr/bin/env bash
# Same as repeat_bigint.sh but for the uuid case.
set -euo pipefail
N="${1:-5}"
ROWS="${ROWS:-1000000}"
export PGPASSWORD=postgres

run_one() {
	local bin="$1" port="$2" label="$3"
	local data="/tmp/perf-repeat-${USER}-${label}"
	rm -rf "$data"
	mkdir -p "$data"
	"$bin" "$data" --listen="postgres://0.0.0.0:${port}" >/dev/null 2>&1 &
	local pid=$!
	for _ in $(seq 1 60); do
		psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 && break
		sleep 0.3
	done
	local psql="psql -h 127.0.0.1 -p $port -U postgres -d postgres -v ON_ERROR_STOP=1"
	$psql <<<'
DROP TABLE IF EXISTS sweep_t;
DROP TEXT SEARCH DICTIONARY IF EXISTS sweep_verbatim;
CREATE TEXT SEARCH DICTIONARY sweep_verbatim(template = '"'"'keyword'"'"');
CREATE TABLE sweep_t (pk INTEGER PRIMARY KEY, body VARCHAR);' >/dev/null
	$psql -c 'CREATE INDEX sweep_idx ON sweep_t USING inverted(pk, body sweep_verbatim);' >/dev/null

	local clk
	clk=$(getconf CLK_TCK)
	read_cpu_ticks() {
		local rest=${1#*) }
		echo "$rest" | awk '{print $12, $13}'
	}
	local cpu_before t0 t1 cpu_after
	cpu_before=$(read_cpu_ticks "$(cat /proc/$pid/stat)")
	t0=$(date +%s.%N)
	$psql <<<"INSERT INTO sweep_t SELECT id, md5(id::VARCHAR) FROM range(${ROWS}) AS r(id);" >/dev/null
	t1=$(date +%s.%N)
	cpu_after=$(read_cpu_ticks "$(cat /proc/$pid/stat)")
	local wall cpu
	wall=$(awk -v t0="$t0" -v t1="$t1" 'BEGIN { printf "%.3f", t1 - t0 }')
	cpu=$(awk -v b="$cpu_before" -v a="$cpu_after" -v clk="$clk" '
    BEGIN { split(b, B, " "); split(a, A, " ");
      printf "%.3f", (A[1] - B[1] + A[2] - B[2]) / clk; }')
	echo "$wall $cpu"
	kill -9 $pid 2>/dev/null || true
	rm -rf "$data"
}

run_set() {
	local bin="$1" port="$2" label="$3"
	echo "== $label ($bin) =="
	local vw=() vc=()
	for i in $(seq 1 "$N"); do
		res=$(run_one "$bin" "$port" "$label")
		wall=$(echo "$res" | awk '{print $1}')
		cpu=$(echo "$res" | awk '{print $2}')
		echo "  run $i: wall=${wall}s cpu=${cpu}s"
		vw+=("$wall")
		vc+=("$cpu")
	done
	local mw mc
	mw=$(printf '%s\n' "${vw[@]}" | awk '{s+=$1} END{printf "%.3f", s/NR}')
	mc=$(printf '%s\n' "${vc[@]}" | awk '{s+=$1} END{printf "%.3f", s/NR}')
	echo "  mean wall=${mw}s cpu=${mc}s"
}

run_set "$(pwd)/../serenedb-main/build_perf/bin/serened" 6193 main
run_set "$(pwd)/build_perf/bin/serened" 6194 mine
