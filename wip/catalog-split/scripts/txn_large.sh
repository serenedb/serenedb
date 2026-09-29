#!/usr/bin/env bash
set -u
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
name=$1
bin=$2
port=$3
reps=${4:-5}
record=${5:-}
data=$S/txl_${name}
rm -rf "${data:?}"
"$bin" "$data" --listen "postgres://0.0.0.0:${port}" >"$S/txl_${name}.out" 2>&1 &
pq() { psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -X -q -At "$@"; }
for _ in $(seq 1 120); do
	pq -c 'select 1' >/dev/null 2>&1 && break
	sleep 0.5
done
pid=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null | head -1)
pq -c "CREATE TEXT SEARCH DICTIONARY bench_dict AS split_text() | normalize_tokens('en_US.UTF-8', accent := false) WITH (frequency, position);" \
	-c "CREATE TABLE bench_txn (id BIGSERIAL PRIMARY KEY, body TEXT);" \
	-c "CREATE INDEX bench_txn_idx ON bench_txn USING inverted(body bench_dict);"
stmt="BEGIN; INSERT INTO bench_txn(body) SELECT 'tok' || g FROM generate_series(1, 100000) g; COMMIT;"
times=()
for r in $(seq 1 "$reps"); do
	pq -c "TRUNCATE bench_txn;"
	sleep 0.3
	if [[ -n "$record" && "$r" == "$reps" ]]; then
		perf record -g -F 2000 -p "$pid" -o "$S/txl_${name}.perf" -- bash -c "psql -h 127.0.0.1 -p $port -U postgres -d postgres -X -q -c \"$stmt\"" >/dev/null 2>&1
	fi
	t0=$(date +%s%N)
	pq -c "$stmt"
	t1=$(date +%s%N)
	times+=($(((t1 - t0) / 1000000)))
done
echo "$name ms: ${times[*]}"
kill -9 "$pid" 2>/dev/null
