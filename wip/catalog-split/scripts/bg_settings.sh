#!/usr/bin/env bash
# Verifies refresh/compaction background settings for inverted indexes and search tables.
PORT=${PORT:-6533}
q() { psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -X -At -v ON_ERROR_STOP=1 "$@"; }
seg() { q -c "SELECT value FROM sdb_metrics WHERE metric = 'num_segments' AND relation_id = (SELECT oid FROM pg_class WHERE relname = '$1')"; }
opt() { q -c "SELECT reloptions FROM pg_class WHERE relname = '$1'" | tr ',' '\n' | grep "^{\?$2=" | tr -d '{}'; }
batches() { # $1 table, $2 first id, $3 count
	for i in $(seq "$2" $(($2 + $3 - 1))); do
		q -c "INSERT INTO $1 VALUES ($i, 'word$i common')" -c "VACUUM (REFRESH_TABLE) $1" >/dev/null
	done
}
visible() { q -c "SELECT count(*) FROM $1 WHERE t @@ ts_like('common')"; }
check() { # $1 name, $2 condition result (0 = pass)
	if [[ $2 -eq 0 ]]; then echo "PASS $1"; else echo "FAIL $1"; fi
}

q -c "DROP TABLE IF EXISTS ci CASCADE" -c "DROP TABLE IF EXISTS cj CASCADE" -c "DROP TABLE IF EXISTS ck CASCADE" -c "DROP TABLE IF EXISTS cr CASCADE" \
	-c "DROP TABLE IF EXISTS st1 CASCADE" -c "DROP TABLE IF EXISTS st2 CASCADE" -c "DROP TABLE IF EXISTS st3 CASCADE" >/dev/null

echo "== inverted index"
q -c "CREATE TABLE ci(id INTEGER, t TEXT)" -c "CREATE INDEX ci_idx ON ci USING inverted(t)" >/dev/null
batches ci 1 6
sleep 4
s=$(seg ci_idx); echo "default: segments after 6 refreshed batches + 4s = $s"
check "I1 default compaction merges" $([[ $s -lt 6 ]]; echo $?)

q -c "CREATE TABLE cj(id INTEGER, t TEXT)" -c "CREATE INDEX cj_idx ON cj USING inverted(t) WITH (compaction_interval = 0)" >/dev/null
batches cj 1 6
sleep 4
s=$(seg cj_idx); echo "WITH compaction_interval=0: segments = $s"
check "I2 WITH compaction_interval=0 keeps segments" $([[ $s -ge 6 ]]; echo $?)

q -c "ALTER INDEX cj_idx SET (compaction_interval = 200)" >/dev/null
sleep 4
s=$(seg cj_idx); echo "after ALTER SET compaction_interval=200: segments = $s, option = $(opt cj_idx compaction_interval)"
check "I3 ALTER enables compaction on an existing index" $([[ $s -lt 6 ]]; echo $?)

q -c "ALTER INDEX ci_idx SET (compaction_interval = 0)" >/dev/null
before=$(seg ci_idx); batches ci 100 6; sleep 4
s=$(seg ci_idx); echo "after ALTER SET compaction_interval=0: segments $before -> $s"
check "I4 ALTER disables compaction on an existing index" $([[ $s -ge $((before + 6)) ]]; echo $?)

q -c "ALTER INDEX ci_idx RESET (compaction_interval)" >/dev/null
sleep 4
s=$(seg ci_idx); echo "after RESET: segments = $s, option = $(opt ci_idx compaction_interval)"
check "I5 RESET returns to the default and compacts" $([[ $s -lt $((before + 6)) ]]; echo $?)

q -c "SET compaction_interval = 0" -c "CREATE TABLE ck(id INTEGER, t TEXT)" -c "CREATE INDEX ck_idx ON ck USING inverted(t)" >/dev/null
o=$(opt ck_idx compaction_interval); echo "session SET compaction_interval=0 then CREATE INDEX: option = $o"
check "I6 session default reaches a new index" $([[ $o == "compaction_interval=0" ]]; echo $?)

q -c "BEGIN" -c "ALTER INDEX cj_idx SET (compaction_interval = 0)" -c "ROLLBACK" >/dev/null
o=$(opt cj_idx compaction_interval); echo "after rolled-back ALTER: option = $o"
check "I7 rolled-back ALTER is undone" $([[ $o == "compaction_interval=200" ]]; echo $?)
before=$(seg cj_idx); batches cj 100 6; sleep 4; s=$(seg cj_idx)
echo "after rolled-back ALTER: segments $before + 6 batches -> $s"
check "I7b rolled-back ALTER leaves compaction running" $([[ $s -lt $((before + 6)) ]]; echo $?)

q -c "CREATE TABLE cr(id INTEGER, t TEXT)" -c "CREATE INDEX cr_idx ON cr USING inverted(t) WITH (refresh_interval = 0)" >/dev/null
q -c "INSERT INTO cr SELECT g, 'common' FROM generate_series(1, 10) g" >/dev/null
sleep 3; v=$(visible cr); echo "WITH refresh_interval=0: visible after 3s = $v"
check "R1 refresh_interval=0 does not auto-refresh" $([[ $v -eq 0 ]]; echo $?)
q -c "ALTER INDEX cr_idx SET (refresh_interval = 200)" >/dev/null
sleep 3; v=$(visible cr); echo "after ALTER SET refresh_interval=200: visible = $v"
check "R2 ALTER enables refresh on an existing index" $([[ $v -eq 10 ]]; echo $?)
q -c "ALTER INDEX cr_idx SET (refresh_interval = 0)" -c "INSERT INTO cr SELECT g, 'common' FROM generate_series(11, 20) g" >/dev/null
sleep 3; v=$(visible cr); echo "after ALTER SET refresh_interval=0: visible = $v"
check "R3 ALTER disables refresh on an existing index" $([[ $v -eq 10 ]]; echo $?)
q -c "ALTER INDEX cr_idx RESET (refresh_interval)" >/dev/null
sleep 3; v=$(visible cr); echo "after RESET refresh_interval: visible = $v"
check "R4 RESET refresh_interval resumes refresh" $([[ $v -eq 20 ]]; echo $?)

echo "== search table"
q -c "CREATE TABLE st1(id INTEGER, t TEXT) WITH (storage = 'search')" >/dev/null
batches st1 1 6; sleep 4; s=$(seg st1); echo "default search table: segments = $s"
check "S1 default compaction merges" $([[ $s -lt 6 ]]; echo $?)

q -c "CREATE TABLE st2(id INTEGER, t TEXT) WITH (storage = 'search', compaction_interval = 0)" >/dev/null
batches st2 1 6; sleep 4; s=$(seg st2); echo "WITH compaction_interval=0: segments = $s"
check "S2 WITH compaction_interval=0 keeps segments" $([[ $s -ge 6 ]]; echo $?)
q -c "ALTER TABLE st2 SET (compaction_interval = 200)" >/dev/null
sleep 4; s=$(seg st2); echo "after ALTER TABLE SET compaction_interval=200: segments = $s, option = $(opt st2 compaction_interval)"
check "S3 ALTER enables compaction on an existing table" $([[ $s -lt 6 ]]; echo $?)
q -c "ALTER TABLE st1 SET (compaction_interval = 0)" >/dev/null
before=$(seg st1); batches st1 100 6; sleep 4; s=$(seg st1)
echo "after ALTER TABLE SET compaction_interval=0: segments $before -> $s"
check "S4 ALTER disables compaction on an existing table" $([[ $s -ge $((before + 6)) ]]; echo $?)
q -c "ALTER TABLE st1 RESET (compaction_interval)" >/dev/null
sleep 4; s=$(seg st1); echo "after RESET: segments = $s, option = $(opt st1 compaction_interval)"
check "S5 RESET returns to the default and compacts" $([[ $s -lt $((before + 6)) ]]; echo $?)

q -c "CREATE TABLE st3(id INTEGER, t TEXT) WITH (storage = 'search', refresh_interval = 0)" >/dev/null
q -c "INSERT INTO st3 SELECT g, 'common' FROM generate_series(1, 10) g" >/dev/null
sleep 3; v=$(visible st3); echo "search table refresh_interval=0: visible = $v"
check "SR1 refresh_interval=0 does not auto-refresh" $([[ $v -eq 0 ]]; echo $?)
q -c "ALTER TABLE st3 SET (refresh_interval = 200)" >/dev/null
sleep 3; v=$(visible st3); echo "after ALTER TABLE SET refresh_interval=200: visible = $v"
check "SR2 ALTER enables refresh on an existing table" $([[ $v -eq 10 ]]; echo $?)
q -c "ALTER TABLE st3 SET (refresh_interval = 0)" -c "INSERT INTO st3 SELECT g, 'common' FROM generate_series(11, 20) g" >/dev/null
sleep 3; v=$(visible st3); echo "after ALTER TABLE SET refresh_interval=0: visible = $v"
check "SR3 ALTER disables refresh on an existing table" $([[ $v -eq 10 ]]; echo $?)
q -c "ALTER TABLE st3 RESET (refresh_interval)" >/dev/null
sleep 3; v=$(visible st3); echo "after RESET: visible = $v"
check "SR4 RESET resumes refresh" $([[ $v -eq 20 ]]; echo $?)
