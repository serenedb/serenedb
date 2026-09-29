#!/bin/bash
BIN=$1; P=$2; D=$3; LOG=$4
rm -rf "$D"
start() { ("$BIN" "$D" --listen="postgres://0.0.0.0:$P" >> "$LOG" 2>&1 &); for i in $(seq 1 60); do psql -h 127.0.0.1 -p $P -U postgres -d postgres -Atc 'select 1' >/dev/null 2>&1 && return 0; sleep 0.5; done; return 1; }
start || { echo "first start failed"; exit 1; }
psql -h 127.0.0.1 -p $P -U postgres -d postgres -Atc "CREATE DATABASE zz_probe_db" -c "CREATE TABLE zz_probe_db.public.t (x INTEGER)" -c "INSERT INTO zz_probe_db.public.t VALUES (1)" -c "DROP DATABASE zz_probe_db" 2>&1 | tail -2
kill -9 $(lsof -t -i:$P) 2>/dev/null; sleep 1
if start; then echo "restart OK: $(psql -h 127.0.0.1 -p $P -U postgres -d postgres -Atc "SELECT count(*) FROM pg_database WHERE datname='zz_probe_db'")"; else echo "restart FAILED"; grep -E 'FATAL' "$LOG" | tail -1 | cut -c1-250; fi
kill -9 $(lsof -t -i:$P) 2>/dev/null
