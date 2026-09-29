#!/usr/bin/env bash
PORT=${PORT:-6533}
q() { psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -X -At -v ON_ERROR_STOP=1 "$@"; }
seg() { q -c "SELECT value FROM sdb_metrics WHERE metric = 'num_segments' AND relation_id = (SELECT oid FROM pg_class WHERE relname = '$1')"; }
opt() { q -c "SELECT reloptions FROM pg_class WHERE relname = '$1'" | tr ',' '\n' | grep "^{\?$2=" | tr -d '{}'; }
batches() { for i in $(seq "$2" $(($2 + $3 - 1))); do q -c "INSERT INTO $1 VALUES ($i, 'word$i common')" -c "VACUUM (REFRESH_TABLE) $1" >/dev/null; done; }
check() { if [[ $2 -eq 0 ]]; then echo "PASS $1"; else echo "FAIL $1"; fi; }

q -c "DROP TABLE IF EXISTS rb CASCADE" -c "DROP TABLE IF EXISTS rbs CASCADE" -c "DROP TABLE IF EXISTS cr CASCADE" -c "DROP TABLE IF EXISTS st3 CASCADE" >/dev/null

echo "== rolled-back ALTER, inverted index"
q -c "CREATE TABLE rb(id INTEGER, t TEXT)" -c "CREATE INDEX rb_idx ON rb USING inverted(t) WITH (compaction_interval = 200)" >/dev/null
q -c "BEGIN" -c "ALTER INDEX rb_idx SET (compaction_interval = 0)" -c "ROLLBACK" >/dev/null
echo "option after rollback: $(opt rb_idx compaction_interval)"
batches rb 1 6; sleep 6; s=$(seg rb_idx); echo "segments 6s after 6 batches: $s"
check "B1 compaction still runs after a rolled-back disable" $([[ $s -lt 6 ]]; echo $?)
q -c "ALTER INDEX rb_idx SET (compaction_interval = 0)" >/dev/null
q -c "BEGIN" -c "ALTER INDEX rb_idx SET (compaction_interval = 200)" -c "ROLLBACK" >/dev/null
before=$(seg rb_idx); batches rb 100 6; sleep 6; s=$(seg rb_idx)
echo "option $(opt rb_idx compaction_interval), segments $before + 6 batches -> $s"
check "B2 compaction stays off after a rolled-back enable" $([[ $s -ge $((before + 6)) ]]; echo $?)

echo "== rolled-back ALTER, search table"
q -c "CREATE TABLE rbs(id INTEGER, t TEXT) WITH (storage = 'search', compaction_interval = 200)" >/dev/null
q -c "BEGIN" -c "ALTER TABLE rbs SET (compaction_interval = 0)" -c "ROLLBACK" >/dev/null
echo "option after rollback: $(opt rbs compaction_interval)"
batches rbs 1 6; sleep 6; s=$(seg rbs); echo "segments 6s after 6 batches: $s"
check "B3 search table compaction still runs after a rolled-back disable" $([[ $s -lt 6 ]]; echo $?)

echo "== refresh, inverted index (read through the index relation)"
q -c "CREATE TABLE cr(id INTEGER, t TEXT)" -c "CREATE INDEX cr_idx ON cr USING inverted(t) WITH (refresh_interval = 0)" >/dev/null
q -c "INSERT INTO cr SELECT g, 'common' FROM generate_series(1, 10) g" >/dev/null
sleep 3; v=$(q -c "SELECT count(*) FROM cr_idx"); echo "refresh_interval=0: visible = $v"
check "R1 refresh_interval=0 does not auto-refresh" $([[ $v -eq 0 ]]; echo $?)
q -c "ALTER INDEX cr_idx SET (refresh_interval = 200)" >/dev/null
sleep 3; v=$(q -c "SELECT count(*) FROM cr_idx"); echo "after SET refresh_interval=200: visible = $v"
check "R2 ALTER enables refresh on an existing index" $([[ $v -eq 10 ]]; echo $?)
q -c "ALTER INDEX cr_idx SET (refresh_interval = 0)" -c "INSERT INTO cr SELECT g, 'common' FROM generate_series(11, 20) g" >/dev/null
sleep 3; v=$(q -c "SELECT count(*) FROM cr_idx"); echo "after SET refresh_interval=0: visible = $v"
check "R3 ALTER disables refresh on an existing index" $([[ $v -eq 10 ]]; echo $?)
q -c "ALTER INDEX cr_idx RESET (refresh_interval)" >/dev/null
sleep 3; v=$(q -c "SELECT count(*) FROM cr_idx"); echo "after RESET: visible = $v"
check "R4 RESET resumes refresh" $([[ $v -eq 20 ]]; echo $?)

echo "== refresh, search table"
q -c "CREATE TABLE st3(id INTEGER, t TEXT) WITH (storage = 'search', refresh_interval = 0)" >/dev/null
q -c "INSERT INTO st3 SELECT g, 'common' FROM generate_series(1, 10) g" >/dev/null
sleep 3; v=$(q -c "SELECT count(*) FROM st3"); echo "refresh_interval=0: visible = $v"
check "SR1 refresh_interval=0 does not auto-refresh" $([[ $v -eq 0 ]]; echo $?)
q -c "ALTER TABLE st3 SET (refresh_interval = 200)" >/dev/null
sleep 3; v=$(q -c "SELECT count(*) FROM st3"); echo "after SET refresh_interval=200: visible = $v"
check "SR2 ALTER enables refresh on an existing table" $([[ $v -eq 10 ]]; echo $?)
q -c "ALTER TABLE st3 SET (refresh_interval = 0)" -c "INSERT INTO st3 SELECT g, 'common' FROM generate_series(11, 20) g" >/dev/null
sleep 3; v=$(q -c "SELECT count(*) FROM st3"); echo "after SET refresh_interval=0: visible = $v"
check "SR3 ALTER disables refresh on an existing table" $([[ $v -eq 10 ]]; echo $?)
q -c "ALTER TABLE st3 RESET (refresh_interval)" >/dev/null
sleep 3; v=$(q -c "SELECT count(*) FROM st3"); echo "after RESET: visible = $v"
check "SR4 RESET resumes refresh" $([[ $v -eq 20 ]]; echo $?)
