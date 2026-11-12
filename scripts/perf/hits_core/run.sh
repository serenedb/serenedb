#!/usr/bin/env bash
set -euo pipefail
if [ $# -lt 2 ]; then
	echo "usage: run.sh <main_duck|main_search|branch> <validate|full> [setup,bulk,reads,dml,small_plans]" >&2
	exit 2
fi
source "$(dirname "$0")/lib.sh"
source "$HB_DIR/sql.sh"

SIDE=$1
MODE=$2
PHASES=${3:-setup,bulk,reads,dml,small_plans}
WITH=$(side_with "$SIDE")
full_or_validate

if [ "$MODE" = validate ]; then
	FILES="'$HITS_DIR/hits_0.parquet'"
	LIMIT=' WHERE "EventTime" % 10 = 0'
	APPEND_LIMIT=' WHERE "EventTime" % 50 = 0'
else
	FILES=$(hits_files "$HITS_FILES")
	LIMIT=""
	APPEND_LIMIT=""
fi
APPEND_FILE="$HITS_DIR/hits_$((HITS_FILES - 1)).parquet"

OUT=$HB_RESULTS/$SIDE.$MODE
rm -rf "$OUT"
mkdir -p "$OUT"
RESULTS=$OUT/results.tsv
CHECKS=$OUT/checks.txt
: >"$RESULTS"
: >"$CHECKS"
DATA=$(mktemp -d -p "$HB_DATA_ROOT" "data.$SIDE.XXXX")
cleanup() {
	stop_server
	rm -rf "$DATA"
}
trap cleanup EXIT
start_server "$SIDE" "$DATA" "$OUT/serened.log"
echo "side $SIDE mode $MODE port $PORT pid $SERVER_PID"

SRC=$(parquet_src "$FILES")
SEL=$(hits_select)
DDL=$(hits_ddl_cols)
COLS=$(hits_cols)
S_COLS=$(hits_cols s.)
TERMS=(смотреть онлайн бесплатно рецепт)

has() {
	case ",$PHASES," in *",$1,"*) return 0 ;; esac
	return 1
}

check() {
	{
		echo "== $1"
		"${PSQL[@]}" -c "$2" 2>&1 || true
	} >>"$CHECKS"
}

explain() {
	if [ "$MODE" = validate ]; then
		check "EXPLAIN $1" "EXPLAIN $2"
	fi
}

counts() {
	if [ "$MODE" = validate ]; then
		check "counts after $1" "SELECT '$1' AS step, (SELECT count(*) FROM hits) AS hits, (SELECT count(*) FROM hits_pk) AS hits_pk, (SELECT count(DISTINCT watchid) FROM hits_pk) AS pk_ids, (SELECT sum(regionid::BIGINT) FROM hits_pk) AS pk_regions"
	fi
}

series() {
	local name=$1 sql=$2 out times first median args=(-c '\timing on')
	for _ in $(seq 1 "$REPEAT"); do
		args+=(-c "$sql")
	done
	if ! out=$("${PSQL[@]}" "${args[@]}" 2>&1); then
		record "$name" FAIL "$(printf '%s' "$out" | tr '\n\t' '  ' | cut -c1-300)"
		echo "FAIL $name: $out" >&2
		return 0
	fi
	times=$(printf '%s\n' "$out" | awk '/^Time: /{print $2}')
	first=$(printf '%s\n' "$times" | head -1)
	median=$(printf '%s\n' "$times" | sort -n | awk '{a[NR] = $1} END{print a[int((NR + 1) / 2)]}')
	record "$name" "$first" "$median"
	echo "$name: first ${first} ms, median ${median} ms"
}

column_queries() {
	local tag=$1 table=$2
	series "$tag.cq_count" "SELECT count(*) FROM $table WHERE advengineid <> 0"
	series "$tag.cq_users" "SELECT regionid, count(DISTINCT userid) AS u FROM $table GROUP BY regionid ORDER BY u DESC LIMIT 10"
	series "$tag.cq_phrases" "SELECT searchphrase, count(*) AS c FROM $table WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10"
}

index_queries() {
	local tag=$1 index=$2 term
	for term in "${TERMS[@]}"; do
		series "$tag.iq_count.$term" "SELECT count(*) FROM $index WHERE searchphrase @@ '$term'"
	done
	series "$tag.iq_top10" "SELECT watchid FROM $index WHERE title @@ 'онлайн' ORDER BY bm25(tableoid) DESC LIMIT 10"
}

prepare_server() {
	q "SET GLOBAL preserve_insertion_order = false"
	q "CREATE TEXT SEARCH DICTIONARY hb_dict AS split_text(case := 'lower') WITH (frequency, position)"
}

if has setup; then
	{
		echo "== version"
		qa "SELECT version()"
	} >>"$CHECKS"
	prepare_server
	check "settings" "SELECT name, value FROM duckdb_settings() WHERE name IN ('preserve_insertion_order', 'threads', 'memory_limit', 'checkpoint_threshold', 'segment_memory_max', 'default_transaction_isolation', 'transaction_isolation') ORDER BY name"
fi

if has bulk; then
	explain ctas "CREATE TABLE hits$WITH AS SELECT $SEL FROM $SRC$LIMIT"
	run_timed bulk.ctas "CREATE TABLE hits$WITH AS SELECT $SEL FROM $SRC$LIMIT"
	index_timed bulk.index "CREATE INDEX hits_idx ON hits USING inverted (searchphrase hb_dict, title hb_dict)" hits_idx hits
	q "CREATE TABLE hits_pk ($DDL, PRIMARY KEY (watchid))$WITH"
	explain load_pk "INSERT INTO hits_pk SELECT $SEL FROM $SRC$LIMIT"
	run_timed bulk.load_pk "INSERT INTO hits_pk SELECT $SEL FROM $SRC$LIMIT"
	index_timed bulk.index_pk "CREATE INDEX hits_pk_idx ON hits_pk USING inverted (searchphrase hb_dict, title hb_dict)" hits_pk_idx hits_pk
	counts load
fi

if has reads; then
	explain cq_users "SELECT regionid, count(DISTINCT userid) AS u FROM hits_pk GROUP BY regionid ORDER BY u DESC LIMIT 10"
	explain iq_count "SELECT count(*) FROM hits_pk_idx WHERE searchphrase @@ 'смотреть'"
	explain iq_top10 "SELECT watchid FROM hits_pk_idx WHERE title @@ 'онлайн' ORDER BY bm25(tableoid) DESC LIMIT 10"
	if [ "$MODE" = validate ]; then
		for term in "${TERMS[@]}"; do
			check "index vs scan $term" "SELECT (SELECT count(*) FROM hits_pk_idx WHERE searchphrase @@ '$term') AS by_index, (SELECT count(*) FROM hits_pk WHERE list_contains(string_split(lower(searchphrase), ' '), '$term')) AS by_scan"
		done
	fi
	column_queries settled hits_pk
	index_queries settled hits_pk_idx
fi

if has dml; then
	explain update "UPDATE hits_pk SET regionid = regionid + 1 WHERE watchid % 100 = 0"
	run_timed dml.update "UPDATE hits SET regionid = regionid + 1 WHERE watchid % 100 = 0"
	run_timed dml.update_pk "UPDATE hits_pk SET regionid = regionid + 1 WHERE watchid % 100 = 0"
	counts update
	explain delete "DELETE FROM hits_pk WHERE watchid % 100 = 1"
	run_timed dml.delete "DELETE FROM hits WHERE watchid % 100 = 1"
	run_timed dml.delete_pk "DELETE FROM hits_pk WHERE watchid % 100 = 1"
	counts delete
	stage() {
		local name=$1 rest=$2
		q "CREATE TABLE $name AS SELECT $(hits_cols_replace regionid 'regionid + 1') FROM hits_pk WHERE watchid % 100 = $rest UNION ALL SELECT $(hits_cols_replace watchid "watchid - $NEW_ID_SHIFT") FROM hits_pk WHERE watchid % 100 = $rest"
	}
	stage src_up 2
	stage src_rep 3
	stage src_merge 4
	check "staged" "SELECT (SELECT count(*) FROM src_up) AS up, (SELECT count(*) FROM src_rep) AS rep, (SELECT count(*) FROM src_merge) AS merge"
	explain upsert "INSERT INTO hits_pk SELECT * FROM src_up ON CONFLICT (watchid) DO UPDATE SET regionid = excluded.regionid"
	run_timed dml.upsert_pk "INSERT INTO hits_pk SELECT * FROM src_up ON CONFLICT (watchid) DO UPDATE SET regionid = excluded.regionid"
	counts upsert
	explain replace "INSERT OR REPLACE INTO hits_pk SELECT * FROM src_rep"
	run_timed dml.replace_pk "INSERT OR REPLACE INTO hits_pk SELECT * FROM src_rep"
	counts replace
	MERGE_SQL="USING src_merge s ON t.watchid = s.watchid WHEN MATCHED THEN UPDATE SET regionid = s.regionid WHEN NOT MATCHED THEN INSERT ($COLS) VALUES ($S_COLS)"
	explain merge "MERGE INTO hits_pk t $MERGE_SQL"
	run_timed dml.merge "MERGE INTO hits t $MERGE_SQL"
	run_timed dml.merge_pk "MERGE INTO hits_pk t $MERGE_SQL"
	counts merge
	APPEND_SEL=$(hits_select '-"WatchID"')
	APPEND_SRC=$(parquet_src "'$APPEND_FILE'")
	explain append "INSERT INTO hits_pk SELECT $APPEND_SEL FROM $APPEND_SRC$APPEND_LIMIT"
	run_timed dml.append "INSERT INTO hits SELECT $APPEND_SEL FROM $APPEND_SRC$APPEND_LIMIT"
	run_timed dml.append_pk "INSERT INTO hits_pk SELECT $APPEND_SEL FROM $APPEND_SRC$APPEND_LIMIT"
	counts append
	q "UPDATE hits_pk SET regionid = regionid + 1 WHERE watchid % 100 = 5"
	column_queries after_write hits_pk
	q "UPDATE hits_pk SET regionid = regionid + 1 WHERE watchid % 100 = 6"
	index_queries after_write hits_pk_idx
	IDLE_WINDOW=40 settle
	record total.server_cpu "$(echo "scale=1; $(server_ticks) / $CLK_TCK" | bc)" "$SETTLE_S" "$BG_CPU_S"
	echo "total server cpu $(echo "scale=1; $(server_ticks) / $CLK_TCK" | bc) s, final settle ${SETTLE_S} s with ${BG_CPU_S} s cpu"
fi

if has small_plans; then
	load_events() {
		local pk=$1 key=""
		if [ "$pk" = pk ]; then
			key=" PRIMARY KEY"
		fi
		q "DROP TABLE IF EXISTS events"
		q "CREATE TABLE events (id BIGINT$key, counterid INTEGER, regionid INTEGER, userid BIGINT, eventtime BIGINT, url TEXT, title TEXT, searchphrase TEXT)$WITH"
		q "INSERT INTO events SELECT row_number() OVER () AS id, \"CounterID\", \"RegionID\", \"UserID\", \"EventTime\", \"URL\", \"Title\", \"SearchPhrase\" FROM read_parquet('$HITS_DIR/hits_0.parquet', binary_as_string = true) LIMIT 20000"
		q "CREATE INDEX events_idx ON events USING inverted (searchphrase hb_dict, title hb_dict)"
	}
	VALUES="(7, 1, 2, 1, 7, 'u', 't', 'p')"
	MERGE_ONE="MERGE INTO events t USING (SELECT 7::BIGINT AS id, 2::INTEGER AS regionid) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET regionid = s.regionid WHEN NOT MATCHED THEN INSERT (id, counterid, regionid, userid, eventtime, url, title, searchphrase) VALUES (s.id, 1, s.regionid, 1, 1, 'u', 't', 'p')"
	for pk in pk nopk; do
		load_events "$pk"
		explain "small_insert.$pk" "INSERT INTO events VALUES $VALUES"
		explain "small_update.$pk" "UPDATE events SET regionid = 5 WHERE id = 7"
		explain "small_delete.$pk" "DELETE FROM events WHERE id = 7"
		explain "small_merge.$pk" "$MERGE_ONE"
		explain "read_column.$pk" "SELECT regionid, count(*) FROM events GROUP BY regionid ORDER BY 2 DESC LIMIT 10"
		explain "read_index.$pk" "SELECT count(*) FROM events_idx WHERE searchphrase @@ 'онлайн'"
	done
	load_events pk
	explain small_upsert "INSERT INTO events VALUES $VALUES ON CONFLICT (id) DO UPDATE SET regionid = excluded.regionid"
	explain small_replace "INSERT OR REPLACE INTO events VALUES $VALUES"
fi
echo "done: $RESULTS"
