#!/usr/bin/env bash
set -u
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
name=$1
bin=$2
port=$3
reps=${4:-4}
threads=${5:-}
data=$S/txh_${name}
rm -rf "${data:?}"
"$bin" "$data" --listen "postgres://0.0.0.0:${port}" >"$S/txh_${name}.out" 2>&1 &
pq() { psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -X -q -At "$@"; }
for _ in $(seq 1 120); do
	pq -c 'select 1' >/dev/null 2>&1 && break
	sleep 0.5
done
pid=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null | head -1)
pq -c "CREATE TEXT SEARCH DICTIONARY bench_dict AS split_text() | normalize_tokens('en_US.UTF-8', accent := false) WITH (frequency, position);" \
	-c "CREATE TABLE t_heavy (id BIGINT PRIMARY KEY, body TEXT);" \
	-c "CREATE INDEX t_heavy_idx ON t_heavy USING inverted(body bench_dict);"
prefix=""
[[ -n "$threads" ]] && prefix="SET threads = $threads; "
stmt="${prefix}INSERT INTO t_heavy SELECT g, 'lorem ipsum dolor sit amet consectetur ' || md5(g::VARCHAR) || ' adipiscing elit sed do ' || (g % 1000) || ' eiusmod tempor ' || (g % 97) || ' incididunt labore' FROM generate_series(1, 100000) g;"
times=()
for _ in $(seq 1 "$reps"); do
	pq -c "TRUNCATE t_heavy;"
	sleep 0.3
	if [[ -n "${RECORD:-}" && ${#times[@]} -eq 0 ]]; then perf record -g -F 4000 -p "$pid" -o "$S/txh_${name}.perf" -- bash -c "psql -h 127.0.0.1 -p $port -U postgres -d postgres -X -q -c \"$stmt\"" >/dev/null 2>&1; pq -c "TRUNCATE t_heavy;"; sleep 0.3; fi
	t0=$(date +%s%N)
	pq -c "$stmt"
	t1=$(date +%s%N)
	times+=($(((t1 - t0) / 1000000)))
done
echo "$name ms: ${times[*]}"
kill -9 "$pid" 2>/dev/null
