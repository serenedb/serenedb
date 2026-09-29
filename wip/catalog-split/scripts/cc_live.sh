#!/usr/bin/env bash
set -u
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
bin=$1
port=$2
rounds=${3:-40}
data=$S/cc_live_data
rm -rf "${data:?}"
"$bin" "$data" --listen "postgres://0.0.0.0:${port}" >"$S/cc_live_server.log" 2>&1 &
pq() { psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -X -q -At "$@"; }
for _ in $(seq 1 120); do
	pq -c 'select 1' >/dev/null 2>&1 && break
	sleep 0.5
done
pq -c "CREATE TEXT SEARCH DICTIONARY cc_dict AS split_text() | normalize_tokens('en_US.UTF-8', accent := false) WITH (frequency, position);" \
	-c "CREATE TABLE cc_t(id INTEGER PRIMARY KEY, body TEXT);" \
	-c "CREATE INDEX cc_idx ON cc_t USING inverted(id, body cc_dict) WITH (refresh_interval = 1);" \
	-c "SET sdb_faults = 'long_waited_advance';"
writer() {
	local w=$1
	for r in $(seq 0 $((rounds - 1))); do
		local lo=$((w * 1000000 + r * 250))
		pq -c "INSERT INTO cc_t SELECT i, 'tok_${w}_' || i FROM range(${lo}, $((lo + 250))) AS t(i);" || echo "writer $w round $r failed"
	done
}
refresher() {
	for _ in $(seq 1 $((rounds * 2))); do
		pq -c "VACUUM (REFRESH_TABLE) cc_t;" >/dev/null 2>&1
	done
}
for w in 1 2 3 4 5 6 7 8; do writer "$w" & done
refresher &
refresher &
wait
pq -c "VACUUM (REFRESH_TABLE) cc_t;"
table=$(pq -c "SELECT count(*) FROM cc_t;")
index=$(pq -c "SELECT count(*) FROM cc_idx;")
echo "table=$table index=$index expected=$((8 * rounds * 250))"
pid=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null | head -1)
[[ -n "$pid" ]] && kill -9 "$pid"
