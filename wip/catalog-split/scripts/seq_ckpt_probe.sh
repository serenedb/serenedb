#!/bin/bash
BIN=$1; P=$2; D=$3; LOG=$4
rm -rf "$D"
q() { psql -h 127.0.0.1 -p $P -U postgres -d postgres -Atc "$1" 2>&1 | tail -1; }
start() { ("$BIN" "$D" --listen="postgres://0.0.0.0:$P" >> "$LOG" 2>&1 &); for i in $(seq 1 60); do psql -h 127.0.0.1 -p $P -U postgres -d postgres -Atc 'select 1' >/dev/null 2>&1 && return 0; sleep 0.5; done; return 1; }
start || { echo "start failed"; exit 1; }
q "CREATE TABLE t (id BIGSERIAL PRIMARY KEY, v INT)" >/dev/null
for v in 1 2 3; do q "INSERT INTO t (v) VALUES ($v)" >/dev/null; done
q "CHECKPOINT" >/dev/null
kill -9 $(lsof -t -i:$P) 2>/dev/null; sleep 1
start || { echo "restart failed"; exit 1; }
echo "after crash: new id -> $(psql -h 127.0.0.1 -p $P -U postgres -d postgres -Atc "INSERT INTO t (v) VALUES (4) RETURNING id" 2>&1 | head -1)"
kill -9 $(lsof -t -i:$P) 2>/dev/null
