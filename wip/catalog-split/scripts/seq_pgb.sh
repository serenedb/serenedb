#!/usr/bin/env bash
set -u
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
name=$1
bin=$2
port=$3
data=$S/spg_${name}
rm -rf "${data:?}"
"$bin" "$data" --listen "postgres://0.0.0.0:${port}" >"$S/spg_${name}.out" 2>&1 &
pq() { psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -X -q -At "$@"; }
for _ in $(seq 1 120); do
	pq -c 'select 1' >/dev/null 2>&1 && break
	sleep 0.5
done
pq -c "CREATE TEXT SEARCH DICTIONARY bench_dict AS split_text() | normalize_tokens('en_US.UTF-8', accent := false) WITH (frequency, position);" \
	-c "CREATE TABLE bench_seq (id BIGSERIAL PRIMARY KEY, v INTEGER);" \
	-c "CREATE TABLE bench_txn (id BIGSERIAL PRIMARY KEY, body TEXT);" \
	-c "CREATE INDEX bench_txn_idx ON bench_txn USING inverted(body bench_dict);"
echo "INSERT INTO bench_seq (v) VALUES (1);" >"$S/spg_nextval.sql"
printf 'BEGIN;\nINSERT INTO bench_txn(body) SELECT %s FROM generate_series(1, 10) g;\nCOMMIT;\n' "'tok' || g" >"$S/spg_txn_small.sql"
printf 'BEGIN;\nINSERT INTO bench_txn(body) SELECT %s FROM generate_series(1, 10) g;\nROLLBACK;\n' "'tok' || g" >"$S/spg_rollback.sql"
out="$name"
for w in nextval txn_small rollback; do
	mode=prepared
	[[ "$w" != nextval ]] && mode=simple
	tps=$(pgbench -h 127.0.0.1 -p "$port" -U postgres -n -M "$mode" -c 8 -j 4 -T 10 -f "$S/spg_${w}.sql" postgres 2>&1 | awk -F'= ' '/^tps =/{print int($2); exit}')
	out="$out $w=$tps"
done
echo "$out load=$(cut -d' ' -f1 /proc/loadavg)"
pid=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null | head -1)
[[ -n "$pid" ]] && kill -9 "$pid"
