#!/usr/bin/env bash
# 5-iter mean uuid online INSERT, three binaries: main / mine_pre / mine_opt.
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
	awk -v t0="$t0" -v t1="$t1" 'BEGIN { printf "%.3f", t1 - t0 }'
	echo -n ' '
	awk -v b="$cpu_before" -v a="$cpu_after" -v clk="$clk" '
    BEGIN { split(b, B, " "); split(a, A, " ");
      printf "%.3f", (A[1] - B[1] + A[2] - B[2]) / clk; }'
	echo
	kill -9 $pid 2>/dev/null || true
	rm -rf "$data"
}

run_set() {
	local bin="$1" port="$2" label="$3"
	echo "== $label =="
	for i in $(seq 1 "$N"); do
		res=$(run_one "$bin" "$port" "$label")
		echo "  run $i: wall=$(echo $res | cut -d' ' -f1)s cpu=$(echo $res | cut -d' ' -f2)s"
	done
}

REPO=/home/mironov/projects/serenedb/serenedb
run_set "$REPO/../serenedb-main/build_perf/bin/serened" 6193 main
run_set "/tmp/serened_pre" 6194 mine_pre
run_set "/tmp/serened_opt" 6195 mine_opt
run_set "/tmp/serened_sort1" 6196 mine_sort1
run_set "/tmp/serened_sort2" 6197 mine_sort2
run_set "/tmp/serened_term_inline" 6198 mine_term_inline
run_set "/tmp/serened_push_inline" 6199 mine_push_inline
run_set "/tmp/serened_pdq" 6200 mine_pdq
run_set "/tmp/serened_pdq_b" 6201 mine_pdq_b
run_set "/tmp/serened_fieldcache" 6202 mine_fieldcache
