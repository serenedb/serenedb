#!/usr/bin/env bash
set -euo pipefail
if [ $# -ne 5 ]; then
	echo "usage: cell.sh <main_duck|main_search|branch> <validate|full> <pk|nopk> <insert|update|delete|upsert|replace|merge> <none|column|index>" >&2
	exit 2
fi
source "$(dirname "$0")/lib.sh"

SIDE=$1
MODE=$2
PK=$3
OP=$4
READERS_KIND=$5
WITH=$(side_with "$SIDE")
full_or_validate
TERMS=(смотреть онлайн бесплатно рецепт)
OUT=$HB_RESULTS/$SIDE.$MODE
W=$OUT/pgbench
mkdir -p "$W"
RESULTS=$OUT/small.tsv
NAME=small.$PK.$OP.$READERS_KIND
DATA=$(mktemp -d -p "$HB_DATA_ROOT" "data.$SIDE.XXXX")
cleanup() {
	stop_server
	rm -rf "$DATA"
}
trap cleanup EXIT
start_server "$SIDE" "$DATA" "$OUT/serened.$NAME.log"
q "SET GLOBAL preserve_insertion_order = false"
q "CREATE TEXT SEARCH DICTIONARY hb_dict AS split_text(case := 'lower') WITH (frequency, position)"

ROW="(:id, :c, :r, :c, :id, 'https://example.com/' || :r, 'title смотреть ' || :r, 'phrase онлайн ' || :c)"
VARS=$'\\set c random(1, 100000)\n\\set r random(1, 1000)'
case $OP in
insert) printf '%s\n%s\nINSERT INTO events VALUES %s;\n' '\set id random(1000000000, 9000000000000000000)' "$VARS" "$ROW" ;;
update) printf '%s\n%s\nUPDATE events SET regionid = :r WHERE id = :id;\n' "\\set id random(1, $EVENTS_ROWS)" "$VARS" ;;
delete) printf '%s\nDELETE FROM events WHERE id = :id;\n' "\\set id random(1, $EVENTS_ROWS)" ;;
upsert) printf '%s\n%s\nINSERT INTO events VALUES %s ON CONFLICT (id) DO UPDATE SET regionid = excluded.regionid;\n' "\\set id random(1, $((EVENTS_ROWS * 2)))" "$VARS" "$ROW" ;;
replace) printf '%s\n%s\nINSERT OR REPLACE INTO events VALUES %s;\n' "\\set id random(1, $((EVENTS_ROWS * 2)))" "$VARS" "$ROW" ;;
merge) printf '%s\n%s\nMERGE INTO events t USING (SELECT :id::BIGINT AS id, :r::INTEGER AS regionid) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET regionid = s.regionid WHEN NOT MATCHED THEN INSERT (id, counterid, regionid, userid, eventtime, url, title, searchphrase) VALUES (s.id, 1, s.regionid, 1, 1, %s, %s, %s);\n' "\\set id random(1, $((EVENTS_ROWS * 2)))" "$VARS" "'https://example.com/m'" "'title рецепт'" "'phrase бесплатно'" ;;
*)
	echo "unknown operation $OP" >&2
	exit 2
	;;
esac >"$W/$NAME.sql"
echo 'SELECT regionid, count(*) FROM events GROUP BY regionid ORDER BY 2 DESC LIMIT 10;' >"$W/read_column.sql"
READ_INDEX=()
for i in "${!TERMS[@]}"; do
	echo "SELECT count(*) FROM events_idx WHERE searchphrase @@ '${TERMS[$i]}';" >"$W/read_index_$i.sql"
	READ_INDEX+=(-f "$W/read_index_$i.sql")
done

KEY=""
if [ "$PK" = pk ]; then
	KEY=" PRIMARY KEY"
fi
q "CREATE TABLE events (id BIGINT$KEY, counterid INTEGER, regionid INTEGER, userid BIGINT, eventtime BIGINT, url TEXT, title TEXT, searchphrase TEXT)$WITH"
q "INSERT INTO events SELECT row_number() OVER () AS id, \"CounterID\", \"RegionID\", \"UserID\", \"EventTime\", \"URL\", \"Title\", \"SearchPhrase\" FROM read_parquet('$HITS_DIR/hits_0.parquet', binary_as_string = true) LIMIT $EVENTS_ROWS"
q "CREATE INDEX events_idx ON events USING inverted (searchphrase hb_dict, title hb_dict)"
settle

PGB=(pgbench -h 127.0.0.1 -p "$PORT" -U postgres -n -M prepared)
perf_start "$NAME"
c0=$(server_ticks)
rpid=""
case $READERS_KIND in
column)
	"${PGB[@]}" -c "$READERS" -j "$READERS" -T "$PGB_T" -f "$W/read_column.sql" postgres >"$W/$NAME.readers.log" 2>&1 &
	rpid=$!
	;;
index)
	"${PGB[@]}" -c "$READERS" -j "$READERS" -T "$PGB_T" "${READ_INDEX[@]}" postgres >"$W/$NAME.readers.log" 2>&1 &
	rpid=$!
	;;
esac
"${PGB[@]}" -c "$WRITERS" -j "$WRITERS" -T "$PGB_T" --max-tries=10 -f "$W/$NAME.sql" postgres >"$W/$NAME.writers.log" 2>&1 || true
if [ -n "$rpid" ]; then
	wait "$rpid" || true
fi
c1=$(server_ticks)
perf_stop

pgb_field() {
	awk -v k="$2" 'index($0, k) == 1 {sub(k, ""); print $1; exit}' "$1"
}
wtps=$(pgb_field "$W/$NAME.writers.log" "tps = ")
wlat=$(pgb_field "$W/$NAME.writers.log" "latency average = ")
wfail=$(pgb_field "$W/$NAME.writers.log" "number of failed transactions: ")
wretry=$(pgb_field "$W/$NAME.writers.log" "number of transactions retried: ")
wabort=$(grep -c "aborted" "$W/$NAME.writers.log" || true)
rtps=""
rlat=""
rabort=""
if [ -n "$rpid" ]; then
	rtps=$(pgb_field "$W/$NAME.readers.log" "tps = ")
	rlat=$(pgb_field "$W/$NAME.readers.log" "latency average = ")
	rabort=$(grep -c "aborted" "$W/$NAME.readers.log" || true)
fi
cpu=$(echo "scale=1; ($c1 - $c0) / $CLK_TCK" | bc)
rows=$(qa "SELECT count(*) FROM events")
record "$NAME" "${wtps:-NA}" "${wlat:-NA}" "${wfail:-NA}" "${wretry:-NA}" "$wabort" "$rtps" "$rlat" "$rabort" "$cpu" "$rows"
echo "$SIDE $NAME: writers ${wtps:-NA} tps ${wlat:-NA} ms failed ${wfail:-NA} retried ${wretry:-NA} aborted $wabort; readers ${rtps:-} tps ${rlat:-} ms aborted ${rabort:-}; cpu ${cpu} s; rows $rows"
