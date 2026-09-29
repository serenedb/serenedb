#!/usr/bin/env bash
set -u
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
name=$1
bin=$2
port=$3
data=$S/stc_${name}
rm -rf "${data:?}"
"$bin" "$data" --listen "postgres://0.0.0.0:${port}" >"$S/stc_${name}.out" 2>&1 &
pq() { psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -X -q -At "$@"; }
for _ in $(seq 1 120); do
	pq -c 'select 1' >/dev/null 2>&1 && break
	sleep 0.5
done
pq -c "CREATE SEQUENCE bss;"
: >"$S/stc_sel.sql"
: >"$S/stc_nv.sql"
for _ in $(seq 1 2000); do
	echo "SELECT 1;" >>"$S/stc_sel.sql"
	echo "SELECT nextval('bss');" >>"$S/stc_nv.sql"
done
out="$name"
for w in sel nv sel nv; do
	t0=$(date +%s%N)
	pq -f "$S/stc_${w}.sql" >/dev/null
	t1=$(date +%s%N)
	out="$out $w=$(((t1 - t0) / 2000000))us"
done
echo "$out"
pid=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null | head -1)
[[ -n "$pid" ]] && kill -9 "$pid"
