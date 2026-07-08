#!/usr/bin/env bash
# full_matrix.sh -- reusable columnstore benchmark matrix.
#
#   engines:     old  = scripts/perf/results/serened_old  (HEAD~2 build, static)
#                new  = scripts/perf/results/serened_new  (working-tree build)
#                native = DuckDB native table ATTACHed in a `new` serened session
#   benchmarks:  per-type, hits, nested   (read: INCLUDE-column scans)
#                text-gather              (filtered reads: term-density ladder over
#                                          a searchable+included hits index vs
#                                          native LIKE; counts/sums/topk)
#                nested-gather            (filtered nested reads at 0.1/1/10%
#                                          density vs native table filters)
#                search-table             (write/ingest: storage='search' + count)
#   dims:        threads {1,64} x {cold, hot=avg of 2 after cold}
#   metrics:     write time, read time (cold/hot), peak RSS (VmHWM), disk size
#
# Cold = engine restart (drops serened buffer pool) + `vmtouch -e` on the data
# (drops OS page cache) -- no sudo. Before every read phase we wait for
# background compaction to quiesce so timings are not skewed.
#
# Re-runnable: reuses saved binaries + built datadirs/native DBs; pass FM_REBUILD=1
# to force a rebuild of a benchmark's data. Results also written to a timestamped
# file under scripts/perf/results/fm/.
#
# Fresh-machine quickstart (any arch):
#   scripts/perf/download_hits.sh && scripts/perf/sample_hits.sh   # hits parquet
#   cmake --preset perf && ninja -C build_perf bin/serened          # new binary
#     (auto-copied to results/serened_new when missing)
#   FM_OLD_REF=<pre-rewrite ref> scripts/perf/full_matrix.sh        # old binary is
#     built once from that ref into a temporary git worktree when
#     results/serened_old is missing; or drop a prebuilt one there yourself.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
RES="${ROOT}/scripts/perf/results"
FM="${FM_DIR:-${RES}/fm}"
mkdir -p "$FM"
OLD_BIN="${FM_OLD_BIN:-${RES}/serened_old}"
NEW_BIN="${FM_NEW_BIN:-${RES}/serened_new}"
PORT="${FM_PORT:-6299}"
CONN="postgres://postgres@localhost:${PORT}/postgres"
THREAD_SET=(${FM_THREADS:-1 64})
HOT_RUNS="${FM_HOT:-2}"
TYPES_PQ="${FM_TYPES_PQ:-${HOME}/data/types_perf.parquet}"
HITS_PQ="${FM_HITS_PQ:-${HOME}/data/hits_10pct.parquet}"
NESTED_ROWS="${FM_NESTED_ROWS:-10000000}"
NESTED_PQ="${FM_NESTED_PQ:-${FM}/nested_v2.parquet}"
BENCHES="${FM_BENCHES:-per-type hits nested text-gather nested-gather search-table}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || echo run)"
OUT="${FM}/matrix-${STAMP}.tsv"

BUILD_DIR="${FM_BUILD_DIR:-${ROOT}/build_perf}"
if [[ ! -x "$NEW_BIN" && -d "$BUILD_DIR" ]]; then
	echo "building new binary from working tree ($BUILD_DIR)"
	ninja -C "$BUILD_DIR" bin/serened && cp "$BUILD_DIR/bin/serened" "$NEW_BIN"
fi
if [[ ! -x "$OLD_BIN" && -n "${FM_OLD_REF:-}" ]]; then
	OLD_WT="${FM}/old_worktree"
	echo "building old binary from $FM_OLD_REF into $OLD_WT"
	git -C "$ROOT" worktree add -f "$OLD_WT" "$FM_OLD_REF" &&
		cmake --preset "${FM_PRESET:-perf}" -S "$OLD_WT" -B "$OLD_WT/build_perf" &&
		ninja -C "$OLD_WT/build_perf" bin/serened &&
		cp "$OLD_WT/build_perf/bin/serened" "$OLD_BIN"
fi
[[ -x "$OLD_BIN" ]] || {
	echo "missing old binary $OLD_BIN"
	exit 1
}
[[ -x "$NEW_BIN" ]] || {
	echo "missing new binary $NEW_BIN"
	exit 1
}
command -v vmtouch >/dev/null || {
	echo "need vmtouch"
	exit 1
}

: >"$OUT"
# results row: bench<TAB>engine<TAB>phase<TAB>threads<TAB>metric<TAB>value
emit() { printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "$6" >>"$OUT"; }

SERVER_PID=""
start_engine() { # bin datadir
	"$1" "$2" --listen="postgres://0.0.0.0:${PORT}" >"/tmp/${USER}-fm-serened.log" 2>&1 &
	SERVER_PID=$!
	for _ in $(seq 1 120); do
		psql "$CONN" -c 'SELECT 1' >/dev/null 2>&1 && return 0
		sleep 0.5
	done
	echo "engine did not start ($1 $2)"
	tail -20 "/tmp/${USER}-fm-serened.log"
	return 1
}
listener_pid() { lsof -tiTCP:${PORT} -sTCP:LISTEN 2>/dev/null | head -1; }
stop_engine() { # graceful stop, wait for the PROCESS to fully exit (release datadir LOCK)
	local p
	p="$(listener_pid)"
	if [[ -n "$p" ]]; then
		kill "$p" 2>/dev/null
		for _ in $(seq 1 150); do
			kill -0 "$p" 2>/dev/null || break
			sleep 0.2
		done
		if kill -0 "$p" 2>/dev/null; then
			kill -9 "$p" 2>/dev/null
			for _ in $(seq 1 50); do
				kill -0 "$p" 2>/dev/null || break
				sleep 0.2
			done
		fi
	fi
	for _ in $(seq 1 50); do
		listener_pid >/dev/null || break
		sleep 0.2
	done
	SERVER_PID=""
}
peak_rss_mb() {
	local p
	p="$(listener_pid)"
	[[ -z "$p" ]] && {
		echo 0
		return
	}
	awk '/VmHWM/{printf "%.0f", $2/1024}' "/proc/$p/status" 2>/dev/null || echo 0
}
du_mb() { du -sm "$1" 2>/dev/null | awk '{print $1+0}'; }
du_mb_native() {
	local m w
	m=$(du_mb "$1")
	w=$(du_mb "$1.wal")
	echo $((${m:-0} + ${w:-0}))
}
# cs index footprint = engine_search (the .col columnstore + .idx), which is what
# serves reads -- comparable to native's table. Excludes the engine_duckdb base
# table copy (only present when the source is a real TABLE, not a parquet VIEW).
# Sums only LIVE segments (those named in the newest segments_N meta): an
# in-flight merge output or a not-yet-GC'd pre-merge segment would otherwise
# inflate the number (observed: an orphan 2.4GB _146.col written by a merge
# that was still in flight when the engine stopped).
du_mb_index() {
	local d="$1/engine_search" sdir smeta live seg f bytes=0
	[[ -d "$d" ]] || d="$1"
	while IFS= read -r sdir; do
		smeta=$(ls -v "$sdir"/segments_* 2>/dev/null | tail -1)
		if [[ -z "$smeta" ]]; then
			bytes=$((bytes + $(du -sb "$sdir" 2>/dev/null | awk '{print $1+0}')))
			continue
		fi
		live=$(strings "$smeta" | grep -oE '_[0-9]+\.0\.sm' | sed 's/\.0\.sm$//' | sort -u)
		bytes=$((bytes + $(stat -c %s "$smeta")))
		for seg in $live; do
			for f in "$sdir/$seg". "$sdir/$seg".*; do
				[[ -f "$f" ]] && bytes=$((bytes + $(stat -c %s "$f")))
			done
		done
	done < <(find "$d" -name 'segments_*' -printf '%h\n' 2>/dev/null | sort -u)
	echo $((bytes / 1048576))
}
quiesce() { # wait until listener CPU is idle (compaction done)
	local p hz u1 s1 u2 s2 pct idle=0
	p="$(listener_pid)"
	hz=$(getconf CLK_TCK)
	[[ -z "$p" ]] && return
	for _ in $(seq 1 120); do
		read u1 s1 < <(awk '{print $14,$15}' "/proc/$p/stat" 2>/dev/null)
		[[ -z "${u1:-}" ]] && return
		sleep 2
		read u2 s2 < <(awk '{print $14,$15}' "/proc/$p/stat" 2>/dev/null)
		[[ -z "${u2:-}" ]] && return
		pct=$(awk "BEGIN{printf \"%.0f\", (($u2+$s2)-($u1+$s1))/$hz/2*100}")
		if [[ "$pct" -lt 12 ]]; then idle=$((idle + 1)); else idle=0; fi
		[[ "$idle" -ge 3 ]] && return
	done
}
timed() { # threads sql  -> prints ms
	psql "$CONN" -X -q -c "SET threads=$1;" -c "SET search_path TO public, native_db.main;" \
		-c '\timing on' -c "$2" 2>/dev/null | awk '/^Time: /{t=$2} END{print t}'
}
setup_sql() { psql "$CONN" -X -q -v ON_ERROR_STOP=1 "$@"; }

# ---- benchmark definitions: define <bench>_setup/_index/_native/_queries ----
NDB="" # per-bench native duckdb path

per_type_setup() {
	local pq
	pq="$(printf '%s' "$TYPES_PQ" | sed "s/'/''/g")"
	setup_sql -c "CREATE OR REPLACE VIEW bench_view AS SELECT * FROM read_parquet('$pq');"
}
per_type_index() {
	local cols
	cols="$(psql "$CONN" -At -c "SELECT string_agg('\"'||column_name||'\" included()', ', ') FROM (DESCRIBE bench_view);")"
	setup_sql -c "CREATE INDEX bench_idx ON bench_view USING inverted($cols);"
}
per_type_native() {
	local pq
	pq="$(printf '%s' "$TYPES_PQ" | sed "s/'/''/g")"
	setup_sql -c "CREATE TABLE native_db.main.bench_native AS SELECT * FROM read_parquet('$pq');" -c "CHECKPOINT native_db;"
}
per_type_queries() {
	cat <<'EOF'
i64|SUM(i64)
f64|SUM(f64)
varchar|SUM(length(s))
struct|SUM(struct_basic.a)+SUM(length(struct_basic.b))
array|SUM(arr_i32[1]+arr_i32[2]+arr_i32[3])
list|SUM(list_sum(lst_i32))
map|SUM(list_sum(map_values(map_i32)))
variant|SUM(variant_obj.a::INTEGER)+SUM(length(variant_obj.b::VARCHAR))
variantNest|SUM(variant_nested.outer.mid.a::INTEGER)+SUM(length(variant_nested.outer.mid.b::VARCHAR))
EOF
}
per_type_rel_cs="bench_idx"
per_type_rel_nat="bench_native"

hits_setup() {
	local pq
	pq="$(printf '%s' "$HITS_PQ" | sed "s/'/''/g")"
	setup_sql -c "CREATE TEXT SEARCH DICTIONARY fm_english(template='delimiter', delimiter=' ');" 2>/dev/null || true
	setup_sql -c "CREATE OR REPLACE VIEW hits_view AS SELECT * FROM read_parquet('$pq');"
}
hits_index() {
	local cols
	cols="$(psql "$CONN" -At -c "SELECT string_agg('\"'||column_name||'\" included()', ', ') FROM (DESCRIBE hits_view);")"
	setup_sql -c "CREATE INDEX hits_idx ON hits_view USING inverted($cols);"
}
hits_native() {
	local pq
	pq="$(printf '%s' "$HITS_PQ" | sed "s/'/''/g")"
	setup_sql -c "CREATE TABLE native_db.main.hits_native AS SELECT * FROM read_parquet('$pq');" -c "CHECKPOINT native_db;"
}
hits_queries() {
	cat <<'EOF'
sum1|SUM("ResolutionWidth")
groupby|SELECT "RegionID", count(*) c FROM @REL@ GROUP BY "RegionID" ORDER BY c DESC LIMIT 10
distinct|COUNT(DISTINCT "UserID")
strlen|SUM("ResolutionWidth")+SUM("CounterID")+SUM(length("URL"))
EOF
}
hits_rel_cs="hits_idx"
hits_rel_nat="hits_native"

# synthetic nested source, generated ONCE to a parquet so both cs (view) and
# native (CTAS) read the same data with NO base table on either side.
NESTED_SELECT="SELECT i AS pk, (hash(i)%1000)::INTEGER AS h, [(i%100)::FLOAT,((i*7)%100)::FLOAT,((i*13)%100)::FLOAT]::FLOAT[3] AS vec, [(i+0)%50,(i+1)%50,(i+2)%50,(i+3)%50] AS tags, ROW(i*2,'b-'||(i%100)::VARCHAR,(i*0.1)::FLOAT)::STRUCT(a INTEGER,b VARCHAR,c FLOAT) AS meta, MAP {'k1':i,'k2':i*2,'k3':i*3} AS attrs, ROW(i*3,'v-'||(i%50)::VARCHAR)::STRUCT(a INTEGER,b VARCHAR)::VARIANT AS var FROM range(${NESTED_ROWS}) t(i)"
nested_gen() {
	local p
	p="$(printf '%s' "$NESTED_PQ" | sed "s/'/''/g")"
	[[ -f "$NESTED_PQ" ]] || setup_sql -c "COPY ($NESTED_SELECT) TO '$p' (FORMAT parquet);"
}
nested_setup() {
	local p
	nested_gen
	p="$(printf '%s' "$NESTED_PQ" | sed "s/'/''/g")"
	setup_sql -c "CREATE OR REPLACE VIEW nested_src AS SELECT * FROM read_parquet('$p');"
}
nested_index() { setup_sql -c "CREATE INDEX nested_idx ON nested_src USING inverted(vec included(), tags included(), meta included(), attrs included());"; }
nested_native() {
	local p
	p="$(printf '%s' "$NESTED_PQ" | sed "s/'/''/g")"
	setup_sql -c "CREATE TABLE native_db.main.nested_native AS SELECT * FROM read_parquet('$p');" -c "CHECKPOINT native_db;"
}
nested_queries() {
	cat <<'EOF'
array|SUM(vec[1]+vec[2]+vec[3])
list|SUM(list_sum(tags))
struct|SUM(meta.a)+SUM(length(meta.b))+SUM(meta.c)
map|SUM(list_sum(map_values(attrs)))
EOF
}
nested_rel_cs="nested_idx"
nested_rel_nat="nested_native"

# filtered text reads: term-density ladder over a searchable + included hits
# index; native answers the same questions with LIKE over its table. Words
# ascend in selectivity on the ClickBench hits data (Russian titles).
text_gather_setup() {
	local pq
	pq="$(printf '%s' "$HITS_PQ" | sed "s/'/''/g")"
	setup_sql -c "CREATE OR REPLACE VIEW tg_view AS SELECT * FROM read_parquet('$pq');"
}
text_gather_index() {
	setup_sql -c "CREATE TEXT SEARCH DICTIONARY tg_dict(
    template = 'text', locale = 'en_US.UTF-8', case = 'lower',
    stemming = false, accent = false, frequency = true, position = true);"
	# unquoted identifiers: quoted searchable entries fail on view-backed
	# indexes (issue #880); unquoted resolve case-insensitively.
	setup_sql -c "CREATE INDEX tg_idx ON tg_view USING inverted(title tg_dict, resolutionwidth included(), counterid included(), userid included(), url included());"
}
text_gather_native() {
	local pq
	pq="$(printf '%s' "$HITS_PQ" | sed "s/'/''/g")"
	setup_sql -c "CREATE TABLE native_db.main.tg_native AS SELECT * FROM read_parquet('$pq');" -c "CHECKPOINT native_db;"
}
text_gather_queries() {
	local w
	for w in google москва фильм онлайн на; do
		printf '%s\n' \
			"count:$w|SELECT count(*) FROM @REL@ WHERE title @@ ts_phrase('$w')|SELECT count(*) FROM @REL@ WHERE \"Title\" LIKE '%$w%'" \
			"i16:$w|SELECT SUM(resolutionwidth) FROM @REL@ WHERE title @@ ts_phrase('$w')|SELECT SUM(\"ResolutionWidth\") FROM @REL@ WHERE \"Title\" LIKE '%$w%'" \
			"i32:$w|SELECT SUM(counterid) FROM @REL@ WHERE title @@ ts_phrase('$w')|SELECT SUM(\"CounterID\") FROM @REL@ WHERE \"Title\" LIKE '%$w%'" \
			"i64:$w|SELECT SUM(userid) FROM @REL@ WHERE title @@ ts_phrase('$w')|SELECT SUM(\"UserID\") FROM @REL@ WHERE \"Title\" LIKE '%$w%'" \
			"str:$w|SELECT SUM(length(url)) FROM @REL@ WHERE title @@ ts_phrase('$w')|SELECT SUM(length(\"URL\")) FROM @REL@ WHERE \"Title\" LIKE '%$w%'"
	done
	# native has no scoring: its topk rows are a LIMIT floor-cost, not the
	# same answer -- read those two columns as index-vs-scan overhead only.
	for w in фильм москва; do
		printf '%s\n' \
			"bm25:$w|SELECT SUM(rw) FROM (SELECT resolutionwidth AS rw FROM @REL@ WHERE title @@ ts_phrase('$w') ORDER BY BM25(@REL@.tableoid) DESC LIMIT 10)|SELECT SUM(rw) FROM (SELECT \"ResolutionWidth\" AS rw FROM @REL@ WHERE \"Title\" LIKE '%$w%' LIMIT 10)" \
			"tfidf:$w|SELECT SUM(rw) FROM (SELECT resolutionwidth AS rw FROM @REL@ WHERE title @@ ts_phrase('$w') ORDER BY TFIDF(@REL@.tableoid) DESC LIMIT 10)|SELECT SUM(rw) FROM (SELECT \"ResolutionWidth\" AS rw FROM @REL@ WHERE \"Title\" LIKE '%$w%' LIMIT 10)"
	done
}
text_gather_rel_cs="tg_idx"
text_gather_rel_nat="tg_native"

# filtered nested reads at 0.1/1/10% density: the index filters h for the
# columnstore, native gets a table filter on the same column.
nested_gather_setup() { nested_setup; }
nested_gather_index() { setup_sql -c "CREATE INDEX ng_idx ON nested_src USING inverted(h, vec included(), tags included(), meta included(), attrs included(), var included());"; }
nested_gather_native() {
	local p
	p="$(printf '%s' "$NESTED_PQ" | sed "s/'/''/g")"
	setup_sql -c "CREATE TABLE IF NOT EXISTS native_db.main.ng_native AS SELECT * FROM read_parquet('$p');" -c "CHECKPOINT native_db;"
}
nested_gather_queries() {
	local pred d
	for pred in "h = 5" "h < 10" "h < 100"; do
		case "$pred" in "h = 5") d=0.1 ;; "h < 10") d=1 ;; *) d=10 ;; esac
		printf '%s\n' \
			"array:${d}%|SELECT SUM(vec[1]+vec[2]+vec[3]) FROM @REL@ WHERE $pred" \
			"list:${d}%|SELECT SUM(list_sum(tags)) FROM @REL@ WHERE $pred" \
			"struct:${d}%|SELECT SUM(meta.a) FROM @REL@ WHERE $pred" \
			"map:${d}%|SELECT SUM(list_sum(map_values(attrs))) FROM @REL@ WHERE $pred" \
			"variant:${d}%|SELECT SUM(var.a::INTEGER) FROM @REL@ WHERE $pred"
	done
}
nested_gather_rel_cs="ng_idx"
nested_gather_rel_nat="ng_native"

echo "full-matrix run @ $STAMP  ->  $OUT"
echo "old=$OLD_BIN new=$NEW_BIN threads={${THREAD_SET[*]}} hot=$HOT_RUNS"

qsql() { # query-expr relation -> full SQL
	case "$1" in
	SELECT*) echo "${1//@REL@/$2}" ;;
	*) echo "SELECT $1 FROM $2" ;;
	esac
}

# read matrix for one relation on the currently-running engine.
# tag = engine name (old/new/native); datapath = what to vmtouch-evict for cold.
read_matrix() { # bench tag rel datapath restart_cmd
	local bench="$1" tag="$2" rel="$3" datapath="$4" restart="$5" q lbl expr sql ms i sum
	local qfn="${bench//-/_}_queries"
	quiesce
	for th in "${THREAD_SET[@]}"; do
		# cold: restart engine (drop buffer pool) + finish compaction + evict OS cache
		eval "$restart"
		quiesce
		vmtouch -e "$datapath" >/dev/null 2>&1
		while IFS='|' read -r lbl expr nexpr; do
			[[ -z "$lbl" ]] && continue
			[[ "$tag" == native && -n "$nexpr" ]] && expr="$nexpr"
			sql="$(qsql "$expr" "$rel")"
			ms="$(timed "$th" "$sql")"
			emit "$bench" "$tag" "$lbl" "$th" cold "${ms:-NA}"
			sum=0
			for i in $(seq 1 "$HOT_RUNS"); do
				ms="$(timed "$th" "$sql")"
				sum=$(awk "BEGIN{print $sum+${ms:-0}}")
			done
			emit "$bench" "$tag" "$lbl" "$th" hot "$(awk "BEGIN{printf \"%.1f\", $sum/$HOT_RUNS}")"
		done < <($qfn)
		emit "$bench" "$tag" _RSSr "$th" - "$(peak_rss_mb)"
	done
}

run_cs_engine() { # bench engine bin
	local bench="$1" engine="$2" bin="$3"
	local dd="${FM}/${bench}_${engine}_data"
	local setup="${bench//-/_}_setup" index="${bench//-/_}_index"
	local rel_var="${bench//-/_}_rel_cs"
	local rel="${!rel_var}"
	if [[ "${FM_REBUILD:-0}" == 1 || ! -d "$dd" ]]; then
		rm -rf "$dd"
		mkdir -p "$dd"
		start_engine "$bin" "$dd" || return 1
		$setup
		local t0 t1
		t0=$(date +%s.%N)
		$index
		t1=$(date +%s.%N)
		emit "$bench" "$engine" _WRITE 0 - "$(awk "BEGIN{printf \"%.0f\", ($t1-$t0)*1000}")"
		emit "$bench" "$engine" _RSSw 0 - "$(peak_rss_mb)"
		quiesce
		emit "$bench" "$engine" _DISK 0 - "$(du_mb_index "$dd")"
	else
		start_engine "$bin" "$dd" || return 1
		emit "$bench" "$engine" _DISK 0 - "$(du_mb_index "$dd")"
	fi
	read_matrix "$bench" "$engine" "$rel" "$dd" "stop_engine; start_engine '$bin' '$dd'"
	stop_engine
}

run_native() { # bench
	local bench="$1"
	local dd="${FM}/${bench}_native_scratch"
	local ndb="${FM}/${bench}_native.duckdb"
	local setup="${bench//-/_}_setup" nat="${bench//-/_}_native"
	local rel_var="${bench//-/_}_rel_nat"
	local rel="native_db.main.${!rel_var}"
	local ndb_esc
	ndb_esc="$(printf '%s' "$ndb" | sed "s/'/''/g")"
	local rebuild=0
	[[ "${FM_REBUILD:-0}" == 1 || ! -f "$ndb" ]] && rebuild=1
	rm -rf "$dd"
	mkdir -p "$dd"
	[[ "$rebuild" == 1 ]] && rm -f "$ndb" "$ndb.wal" # remove BEFORE any ATTACH
	start_engine "$NEW_BIN" "$dd" || return 1
	setup_sql -c "ATTACH IF NOT EXISTS '$ndb_esc' AS native_db (TYPE duckdb, STORAGE_VERSION latest);"
	if [[ "$rebuild" == 1 ]]; then
		$setup
		local t0 t1
		t0=$(date +%s.%N)
		$nat
		t1=$(date +%s.%N)
		emit "$bench" native _WRITE 0 - "$(awk "BEGIN{printf \"%.0f\", ($t1-$t0)*1000}")"
		emit "$bench" native _RSSw 0 - "$(peak_rss_mb)"
		# force persistence to the .duckdb so it survives the cold-restart re-ATTACH
		setup_sql -c "CHECKPOINT native_db;" -c "DETACH native_db;"
		setup_sql -c "ATTACH IF NOT EXISTS '$ndb_esc' AS native_db (TYPE duckdb, STORAGE_VERSION latest);"
	fi
	emit "$bench" native _DISK 0 - "$(du_mb_native "$ndb")"
	local reattach="stop_engine; start_engine '$NEW_BIN' '$dd'; setup_sql -c \"ATTACH IF NOT EXISTS '$(printf '%s' "$ndb" | sed "s/'/''/g")' AS native_db (TYPE duckdb, STORAGE_VERSION latest);\""
	read_matrix "$bench" native "$rel" "$ndb" "$reattach"
	stop_engine
}

for bench in $BENCHES; do
	[[ "$bench" == "search-table" ]] && continue # handled separately below
	echo "===== benchmark: $bench ====="
	run_cs_engine "$bench" old "$OLD_BIN"
	run_cs_engine "$bench" new "$NEW_BIN"
	run_native "$bench"
done

# ---------- render ----------
echo
echo "################## FULL MATRIX ($STAMP) ##################"
render_bench() {
	local b="$1"
	local T1="${THREAD_SET[0]}" T2="${THREAD_SET[1]:-${THREAD_SET[0]}}"
	echo
	echo "===== $b ====="
	awk -F'\t' -v b="$b" -v t1="$T1" -v t2="$T2" '
    $1==b && $3=="_WRITE"{w[$2]=$6}
    $1==b && $3=="_DISK"{d[$2]=$6}
    $1==b && $3=="_RSSw"{rw[$2]=$6}
    $1==b && $3=="_RSSr"{rr[$2"@"$4]=$6}
    $1==b && ($5=="cold"||$5=="hot"){v[$3"@"$2"@"$4"@"$5]=$6; if(!seen[$3]++) ql[++nq]=$3}
    END{
      printf "  write(ms):   old=%s new=%s native=%s\n", w["old"],w["new"],w["native"]
      printf "  disk(MB):    old=%s new=%s native=%s\n", d["old"],d["new"],d["native"]
      printf "  rss_wr(MB):  old=%s new=%s native=%s\n", rw["old"],rw["new"],rw["native"]
      printf "  rss_rd(MB):  old(t%s=%s t%s=%s) new(t%s=%s t%s=%s) native(t%s=%s t%s=%s)\n", t1,rr["old@"t1],t2,rr["old@"t2],t1,rr["new@"t1],t2,rr["new@"t2],t1,rr["native@"t1],t2,rr["native@"t2]
      printf "\n  %-11s | ------ t%s (old/new/nat) ------ | ------ t%s (old/new/nat) ------\n","",t1,t2
      printf "  %-11s | %8s %8s %8s | %8s %8s %8s\n","query(hot)","old","new","nat","old","new","nat"
      for(i=1;i<=nq;i++){q=ql[i];
        printf "  %-11s | %8s %8s %8s | %8s %8s %8s\n", q,
          v[q"@old@"t1"@hot"],v[q"@new@"t1"@hot"],v[q"@native@"t1"@hot"],
          v[q"@old@"t2"@hot"],v[q"@new@"t2"@hot"],v[q"@native@"t2"@hot"]}
      printf "  %-11s |  (cold t%s)               |  (cold t%s)\n","-- cold --",t1,t2
      for(i=1;i<=nq;i++){q=ql[i];
        printf "  %-11s | %8s %8s %8s | %8s %8s %8s\n", q,
          v[q"@old@"t1"@cold"],v[q"@new@"t1"@cold"],v[q"@native@"t1"@cold"],
          v[q"@old@"t2"@cold"],v[q"@new@"t2"@cold"],v[q"@native@"t2"@cold"]}
    }' "$OUT"
}
for bench in $BENCHES; do
	[[ "$bench" == "search-table" ]] && continue
	render_bench "$bench"
done
echo
echo "raw TSV: $OUT"
