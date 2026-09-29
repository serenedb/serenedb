#!/usr/bin/env bash
set -u
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
name=$1
bin=$2
port=$3
pairs=${4:-50}
data=$S/ddl_fs_${name}
log=$S/ddl_fs_${name}.strace
rm -rf "${data:?}"
sql=$S/ddl_fs_${name}.sql
: >"$sql"
echo "CREATE TABLE bench_seq (id BIGSERIAL PRIMARY KEY, v INTEGER);" >"$S/ddl_fs_${name}.setup"
for i in $(seq 1 "$pairs"); do
	echo "INSERT INTO bench_seq (v) VALUES (1);" >>"$sql"
done
strace -f -y -e trace=fsync,fdatasync,sync_file_range -o "$log" "$bin" "$data" --listen "postgres://0.0.0.0:${port}" >"$S/ddl_fs_${name}.out" 2>&1 &
for _ in $(seq 1 120); do
	psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -Atc 'select 1' >/dev/null 2>&1 && break
	sleep 0.5
done
sleep 2
psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -q -f "$S/ddl_fs_${name}.setup" >/dev/null; sleep 1
n0=$(wc -l <"$log")
t0=$(date +%s%N)
psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -q -v ON_ERROR_STOP=1 -f "$sql" >/dev/null
rc=$?
t1=$(date +%s%N)
sleep 1
n1=$(wc -l <"$log")
pid=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null | head -1)
[[ -n "$pid" ]] && kill -9 "$pid"
wait
echo "$name rc=$rc statements=$pairs ms=$(((t1 - t0) / 1000000)) syncs=$((n1 - n0))"
sed -n "$((n0 + 1)),${n1}p" "$log" | grep -o -E '(fsync|fdatasync|sync_file_range)\([0-9]+<[^>]*>' | sed -E "s#\([0-9]+<${data}/?#(#; s#<##" | sort | uniq -c | sort -rn | head -8
