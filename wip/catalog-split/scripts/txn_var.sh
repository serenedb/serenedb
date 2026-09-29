#!/usr/bin/env bash
set -u
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
name=$1
bin=$2
port=$3
reps=${4:-5}
data=$S/txv_${name}
rm -rf "${data:?}"
"$bin" "$data" --listen "postgres://0.0.0.0:${port}" >"$S/txv_${name}.out" 2>&1 &
pq() { psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -X -q -At "$@"; }
for _ in $(seq 1 120); do
	pq -c 'select 1' >/dev/null 2>&1 && break
	sleep 0.5
done
pid=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null | head -1)
pq -c "CREATE TEXT SEARCH DICTIONARY bench_dict AS split_text() | normalize_tokens('en_US.UTF-8', accent := false) WITH (frequency, position);" \
	-c "CREATE TABLE t_serial (id BIGSERIAL PRIMARY KEY, body TEXT);" \
	-c "CREATE TABLE t_idx (id BIGINT PRIMARY KEY, body TEXT);" \
	-c "CREATE INDEX t_idx_idx ON t_idx USING inverted(body bench_dict);" \
	-c "CREATE TABLE t_both (id BIGSERIAL PRIMARY KEY, body TEXT);" \
	-c "CREATE INDEX t_both_idx ON t_both USING inverted(body bench_dict);"
run() {
	local table=$1 stmt=$2 times=()
	for _ in $(seq 1 "$reps"); do
		pq -c "TRUNCATE $table;"
		sleep 0.2
		local t0 t1
		t0=$(date +%s%N)
		pq -c "$stmt"
		t1=$(date +%s%N)
		times+=($(((t1 - t0) / 1000000)))
	done
	echo "$name $table ms: ${times[*]}"
}
run t_serial "INSERT INTO t_serial(body) SELECT 'tok' || g FROM generate_series(1, 100000) g;"
run t_idx "INSERT INTO t_idx SELECT g, 'tok' || g FROM generate_series(1, 100000) g;"
run t_both "INSERT INTO t_both(body) SELECT 'tok' || g FROM generate_series(1, 100000) g;"
kill -9 "$pid" 2>/dev/null
