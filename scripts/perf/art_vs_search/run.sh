#!/usr/bin/env bash
# run.sh -- key lookups through DuckDB's ART (the PRIMARY KEY of a regular
# table) against the term dictionary of a search table, on the same rows, for
# 32-hex-char VARCHAR keys and BIGINT keys.
#
# Modes: hot (pinned threads, after warmup runs), cold (the first query after
# a restart on an evicted data dir and binary) and cold_opened (the same, after
# a probe for another absent key has opened the table).
#
# DuckDB picks the ART index scan when the scan starts, so the plans are
# EXPLAIN ANALYZE (explain/), with each query's scan types in scans.csv.
#
# Env:
#   BIN      serened (default build_perf/bin/serened)
#   DATA     data dir (default /tmp/art-vs-search-$USER)
#   KEEP     1 keeps DATA for the next run, which then skips the load
#   PORT     default 5497
#   ROWS     rows per table (default 20000000)
#   CPUS     taskset list for serened (default 0-31)
#   ROUNDS   timed runs per query and mode (default 7)
#   WARMUP   hot runs before the timed ones (default 3)
#   OUT      output dir (default scripts/perf/results/art_vs_search_<timestamp>)
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$DIR/../../.." && pwd)"
BIN="${BIN:-$REPO/build_perf/bin/serened}"
DATA="${DATA:-/tmp/art-vs-search-$USER}"
KEEP="${KEEP:-0}"
PORT="${PORT:-5497}"
ROWS="${ROWS:-20000000}"
CPUS="${CPUS:-0-31}"
ROUNDS="${ROUNDS:-7}"
WARMUP="${WARMUP:-3}"
OUT="${OUT:-$REPO/scripts/perf/results/art_vs_search_$(date +%Y%m%d_%H%M%S)}"
LOG="$OUT/serened.log"
RAW="$OUT/raw.csv"
export PGPASSWORD=postgres
PSQL=(psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -X -q -tA -v ON_ERROR_STOP=1)

for tool in psql vmtouch taskset; do
	command -v "$tool" >/dev/null || {
		echo "missing $tool" >&2
		exit 2
	}
done
[[ -x "$BIN" ]] || {
	echo "missing $BIN" >&2
	exit 2
}

mkdir -p "$OUT/explain"
PID=""
trap '[[ -n "$PID" ]] && kill -9 "$PID" 2>/dev/null; true' EXIT

start() {
	if [[ -n "$(ss -ltnH "sport = :${PORT}")" ]]; then
		echo "port ${PORT} is taken" >&2
		exit 3
	fi
	taskset -c "$CPUS" "$BIN" "$DATA" --listen="postgres://127.0.0.1:${PORT}" </dev/null >>"$LOG" 2>&1 &
	PID=$!
	for _ in $(seq 1 2400); do
		"${PSQL[@]}" -c 'SELECT 1' >/dev/null 2>&1 && return 0
		if ! kill -0 "$PID" 2>/dev/null; then
			echo "serened exited, see $LOG" >&2
			exit 3
		fi
		sleep 0.25
	done
	echo "serened never became ready" >&2
	exit 3
}

stop() {
	kill "$PID"
	wait "$PID" || true
	PID=""
}

restart_cold() {
	stop
	vmtouch -eq "$DATA" "$BIN"
	start
}

pin() {
	local threads
	threads="$(taskset -c "$CPUS" nproc)"
	"${PSQL[@]}" -c "SET GLOBAL pin_threads = 'on'" \
		-c "SET GLOBAL threads = $((threads - 1))" -c "SET GLOBAL threads = ${threads}" >/dev/null
}

timed() {
	{
		echo '\timing on'
		cat
	} | "${PSQL[@]}" -f - | awk '/^Time: / {print res "," $2; next} {res = $0}'
}

record() {
	local mode="$1" round="$2" id
	while IFS=, read -r id result ms; do
		echo "${QUERY_KEY[$id]},${mode},${round},${ms},${result}"
	done >>"$RAW"
}

top -b -n 2 -d 1 -o %CPU >"$OUT/load_before.txt"
: >"$LOG"
if [[ "$KEEP" == 1 && -f "$DATA.loaded" && "$(<"$DATA.loaded")" == "$ROWS" ]]; then
	start
else
	rm -rf "$DATA" "$DATA.loaded"
	mkdir -p "$DATA"
	start
	t0=$(date +%s.%N)
	sed "s|@ROWS@|${ROWS}|g" "$DIR/schema.sql" | "${PSQL[@]}" -f - >/dev/null
	t1=$(date +%s.%N)
	awk -v a="$t0" -v b="$t1" 'BEGIN { printf "load_s,%.1f\n", b - a }' >"$OUT/load.csv"
	echo "$ROWS" >"$DATA.loaded"
	stop
	start
fi
du -sb "$DATA"/* | sed "s|$DATA/||" >"$OUT/sizes.txt"
"${PSQL[@]}" -F ' ' -c "SELECT 'search_hex', pg_relation_size('search_hex'), 'search_int', pg_relation_size('search_int')" >>"$OUT/sizes.txt"

declare -A LITERAL
while IFS=$'\t' read -r shape query literal; do
	LITERAL["$shape/$query"]="$literal"
done < <(sed "s|@ROWS@|${ROWS}|g" "$DIR/pick.sql" | "${PSQL[@]}" -F $'\t' -f -)

declare -a QUERY_SQL QUERY_KEY QUERY_PROBE
for shape in hex int; do
	for query in hit miss in100 in1000; do
		for tbl in duck search; do
			op="="
			[[ "$query" == in* ]] && op="IN"
			from="FROM ${tbl}_${shape} WHERE k"
			QUERY_SQL+=("SELECT count(*), sum(v) ${from} ${op} (${LITERAL[$shape/$query]});")
			QUERY_PROBE+=("SELECT count(*), sum(v) ${from} = ${LITERAL[$shape/probe]};")
			QUERY_KEY+=("${shape},${query},${tbl}")
		done
	done
done
ALL="$(seq 0 $((${#QUERY_SQL[@]} - 1)))"

echo "shape,query,tbl,mode,round,ms,result" >"$RAW"

pin
for id in $ALL; do
	IFS=, read -r shape query tbl <<<"${QUERY_KEY[$id]}"
	plan="$OUT/explain/${shape}_${query}_${tbl}.txt"
	"${PSQL[@]}" -c "EXPLAIN ANALYZE ${QUERY_SQL[$id]}" >"$plan"
	echo "${QUERY_KEY[$id]},$(grep -oE 'Type: [A-Za-z]+ Scan|Index Filter|HASH_JOIN' "$plan" | sort -u | paste -sd' ')"
done >"$OUT/scans.csv"
for _ in $(seq 1 "$WARMUP"); do
	printf '%s\n' "${QUERY_SQL[@]}" | timed >/dev/null
done
for round in $(seq 1 "$ROUNDS"); do
	printf '%s\n' "${QUERY_SQL[@]}" | timed | paste -d, <(echo "$ALL") - | record hot "$round"
done

for round in $(seq 1 "$ROUNDS"); do
	for id in $ALL; do
		echo ">>> cold ${round}/${ROUNDS} ${QUERY_KEY[$id]}"
		restart_cold
		timed <<<"${QUERY_SQL[$id]}" | sed "s|^|${id},|" | record cold "$round"
		restart_cold
		timed <<<"${QUERY_PROBE[$id]}" >/dev/null
		timed <<<"${QUERY_SQL[$id]}" | sed "s|^|${id},|" | record cold_opened "$round"
	done
done

sed "s|@RAW@|${RAW}|; s|@OUT@|${OUT}|" "$DIR/summary.sql" |
	psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -X -q -v ON_ERROR_STOP=1 -f - | tee "$OUT/summary.txt"
stop
if [[ "$KEEP" != 1 ]]; then
	rm -rf "$DATA" "$DATA.loaded"
fi
