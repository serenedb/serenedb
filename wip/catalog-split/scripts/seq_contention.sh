#!/usr/bin/env bash
set -u
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
name=$1
bin=$2
port=$3
cache_ddl=${4:-}
data=$S/seqc_${name}
rm -rf "${data:?}"
"$bin" "$data" --listen "postgres://0.0.0.0:${port}" >"$S/seqc_${name}.out" 2>&1 &
for _ in $(seq 1 120); do
	psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -Atc 'select 1' >/dev/null 2>&1 && break
	sleep 0.5
done
psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -q -c "CREATE TABLE t_serial (id BIGSERIAL PRIMARY KEY, v INTEGER);" >/dev/null
if [[ -n "$cache_ddl" ]]; then
	psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -q -c "$cache_ddl" -c "CREATE TABLE t_cache (id BIGINT DEFAULT nextval('s_cache') PRIMARY KEY, v INTEGER);" >/dev/null
fi
echo "INSERT INTO t_serial (v) VALUES (1);" >"$S/seqc_serial.sql"
echo "INSERT INTO t_cache (v) VALUES (1);" >"$S/seqc_cache.sql"
for w in serial ${cache_ddl:+cache}; do
	tps=$(pgbench -h 127.0.0.1 -p "$port" -U postgres -n -M prepared -c 8 -j 4 -T 10 -f "$S/seqc_${w}.sql" postgres 2>&1 | awk -F'= ' '/^tps =/{print $2+0; exit}')
	echo "$name $w tps=$tps load=$(cut -d' ' -f1 /proc/loadavg)"
done
pid=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null | head -1)
[[ -n "$pid" ]] && kill -9 "$pid"
wait 2>/dev/null
