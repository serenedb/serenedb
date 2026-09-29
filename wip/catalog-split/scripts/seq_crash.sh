#!/usr/bin/env bash
set -u
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
name=$1
bin=$2
port=$3
data=$S/sqc_${name}
rm -rf "${data:?}"
pq() { psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -X -q -At "$@"; }
start() {
	"$bin" "$data" --listen "postgres://0.0.0.0:${port}" >>"$S/sqc_${name}.out" 2>&1 &
	for _ in $(seq 1 120); do
		pq -c 'select 1' >/dev/null 2>&1 && return
		sleep 0.5
	done
}
crash() {
	local pid
	pid=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null | head -1)
	[[ -n "$pid" ]] && kill -9 "$pid"
	sleep 1
}
start
pq -c "CREATE SEQUENCE s;"
a=$(pq -c "SELECT nextval('s');")
b=$(pq -c "SELECT nextval('s');")
c=$(pq -c "SELECT nextval('s');")
crash
start
d=$(pq -c "SELECT nextval('s');")
pq -c "CREATE TABLE t (id BIGINT DEFAULT nextval('s'), v INT);"
pq -c "INSERT INTO t (v) SELECT 1 FROM range(5);"
e=$(pq -c "SELECT max(id) FROM t;")
crash
start
f=$(pq -c "SELECT nextval('s');")
g=$(printf 'BEGIN;\nSELECT nextval(%s);\nSELECT nextval(%s);\n' "'s'" "'s'" | pq)
crash
start
h=$(pq -c "SELECT nextval('s');")
echo "$name: autocommit $a $b $c | after crash $d | insert max $e | after crash $f | uncommitted $(echo $g | tr '\n' ' ')| after crash $h"
crash
