#!/usr/bin/env bash
# Reusable harness for the iresearch columnstore read-path rewrite.
#
#   cs_rewrite.sh build   -- build a PERSISTENT dataset ONCE: serened data dir
#                            (iresearch index over hits, all columns INCLUDEd
#                            -> .col columnstore) + an attached native DuckDB
#                            table over the same data. Heavy; run once.
#   cs_rewrite.sh bench    -- reuse the existing dataset (no rebuild): restart
#                            serened on it, re-ATTACH native, run the read
#                            suite at threads={1,64} x {cold,hot}, comparing
#                            iresearch (.col) vs native DuckDB. Re-run this
#                            after every read-path rebuild of serened.
#
# Read-path rewrites that keep the on-disk .col format can reuse the built
# dataset directly; only a format/write change needs a rebuild.
#
# Env overrides:
#   CS_PARQUET (default ~/data/hits_10pct.parquet), CS_DATA, CS_NDB, CS_PORT,
#   CS_THREADS (default "1 64"), CS_HOT_REPS (default 5).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BIN="${CS_BIN:-${ROOT}/build_perf/bin/serened}"
PARQUET="${CS_PARQUET:-${HOME}/data/hits_10pct.parquet}"
DATA="${CS_DATA:-${ROOT}/scripts/perf/results/cs10_data}"
NDB="${CS_NDB:-${ROOT}/scripts/perf/results/cs10_native.duckdb}"
PORT="${CS_PORT:-6262}"
THREADS_LIST="${CS_THREADS:-1 64}"
HOT_REPS="${CS_HOT_REPS:-5}"
CONN="postgres://postgres@localhost:${PORT}/postgres"
SLOG="/tmp/${USER}-cs-rewrite-serened.log"

# For `compare`: run the same index-build (write) + read suite on our branch AND
# the released serenedb/serenedb:latest image, under matched docker+network
# conditions (SENG_* env), so the .col columnstore is measured against the old
# one apples-to-apples. SENG_OURS_BIN defaults to $BIN.
SENG_OURS_BIN="${SENG_OURS_BIN:-$BIN}"
export SENG_OURS_BIN
# shellcheck source=scripts/perf/lib_serened_engine.sh
source "$(dirname "$0")/lib_serened_engine.sh"

[[ -x "$BIN" ]] || {
	echo "missing $BIN" >&2
	exit 1
}

# Columnstore-read-path queries, parameterised by table token @T@. These force
# the .col materialiser (scan + aggregate over INCLUDE columns).
declare -a Q_NAME=(sum_1col groupby_region topk_user count_distinct_user proj_4col)
declare -a Q_SQL=(
	'SELECT sum("ResolutionWidth") FROM @T@;'
	'SELECT "RegionID", count(*) c FROM @T@ GROUP BY "RegionID" ORDER BY c DESC LIMIT 10;'
	'SELECT "UserID", count(*) c FROM @T@ GROUP BY "UserID" ORDER BY c DESC LIMIT 10;'
	'SELECT count(DISTINCT "UserID") FROM @T@;'
	'SELECT sum("ResolutionWidth")+sum("CounterID")+sum("RegionID")+sum(length("URL")) FROM @T@;'
)

wait_ready() {
	for _ in $(seq 1 120); do
		psql "$CONN" -c 'SELECT 1' >/dev/null 2>&1 && return 0
		sleep 0.5
	done
	echo "serened never became ready; tail $SLOG:" >&2
	tail -40 "$SLOG" >&2
	return 1
}

launch() { # launch serened on $DATA (must already exist for bench)
	killall -9 serened >/dev/null 2>&1 || true
	sleep 1
	"$BIN" "$DATA" --listen="postgres://0.0.0.0:${PORT}" >"$SLOG" 2>&1 &
	SERENED_PID=$!
	wait_ready
}
shutdown() { kill -9 "${SERENED_PID:-0}" >/dev/null 2>&1 || true; }

# Run one SQL, return last psql "Time: <ms>" value in ms.
timed() { # $1=threads $2=sql
	local out
	out=$(psql "$CONN" -v ON_ERROR_STOP=1 -X \
		-c "SET threads = $1;" \
		-c "SET search_path TO public, native_db.main;" \
		-c '\timing on' \
		-c "$2" 2>&1)
	awk '/^Time: /{t=$2} END{printf "%s", t}' <<<"$out"
}

cmd_build() {
	[[ -f "$PARQUET" ]] || {
		echo "missing $PARQUET (sample_hits.sh)" >&2
		exit 1
	}
	rm -rf "$DATA" "$NDB" "$NDB.wal"
	mkdir -p "$(dirname "$DATA")"
	launch
	local pq
	pq=$(printf '%s' "$PARQUET" | sed "s/'/''/g")
	local ndb
	ndb=$(printf '%s' "$NDB" | sed "s/'/''/g")
	echo "[build] attach native + view + CTAS + index over $PARQUET"
	psql "$CONN" -v ON_ERROR_STOP=1 -X \
		-c "ATTACH '$ndb' AS native_db (TYPE duckdb, STORAGE_VERSION latest);" \
		-c "SET search_path TO public, native_db.main;" \
		-c "CREATE VIEW hits_view AS SELECT * FROM read_parquet('$pq');" \
		-c "CREATE TABLE native_db.main.hits_native AS SELECT * FROM hits_view;" \
		-c "CHECKPOINT native_db;"
	psql "$CONN" -v ON_ERROR_STOP=1 -X -c "
CREATE TEXT SEARCH DICTIONARY perf_english(template='delimiter', delimiter=' ');"
	local inc
	inc=$(psql "$CONN" -At -v ON_ERROR_STOP=1 -X -c \
		"SELECT string_agg('\"'||column_name||'\"', ', ') FROM (DESCRIBE hits_view);")
	[[ -n "$inc" ]] || {
		echo "empty INCLUDE list" >&2
		shutdown
		exit 1
	}
	echo "[build] CREATE INDEX (INCLUDE $(awk -F', ' '{print NF}' <<<"$inc") cols)"
	psql "$CONN" -v ON_ERROR_STOP=1 -X -c '\timing on' -c "
CREATE INDEX hits_idx ON hits_view USING inverted(\"Title\" perf_english) INCLUDE ($inc);"
	shutdown
	echo "[build] done. data=$DATA ndb=$NDB"
	du -sh "$DATA" "$NDB" 2>/dev/null || true
}

cmd_bench() {
	[[ -d "$DATA" ]] || {
		echo "no dataset at $DATA -- run: $0 build" >&2
		exit 1
	}
	printf '%-22s %3s %12s %12s %8s   %12s %12s %8s\n' \
		query th irs_cold nat_cold "c x" irs_hot nat_hot "h x"
	printf '%s\n' "----------------------------------------------------------------------------------------------"
	for th in $THREADS_LIST; do
		launch
		psql "$CONN" -X -c "ATTACH '$(printf '%s' "$NDB" | sed "s/'/''/g")' AS native_db (TYPE duckdb);" >/dev/null 2>&1 || true
		for i in "${!Q_NAME[@]}"; do
			local name="${Q_NAME[$i]}" tmpl="${Q_SQL[$i]}"
			local sql_irs="${tmpl//@T@/hits_idx}"
			local sql_nat="${tmpl//@T@/hits_native}"
			# cold: evict OS cache for the relevant store, then one run.
			vmtouch -e "$DATA" >/dev/null 2>&1 || true
			local ic
			ic=$(timed "$th" "$sql_irs")
			vmtouch -e "$NDB" "$NDB.wal" >/dev/null 2>&1 || true
			local nc
			nc=$(timed "$th" "$sql_nat")
			# hot: min over HOT_REPS.
			local ih="" nh="" t
			for _ in $(seq 1 "$HOT_REPS"); do
				t=$(timed "$th" "$sql_irs")
				ih=$(awk -v a="$ih" -v b="$t" 'BEGIN{print (a==""||b<a)?b:a}')
			done
			for _ in $(seq 1 "$HOT_REPS"); do
				t=$(timed "$th" "$sql_nat")
				nh=$(awk -v a="$nh" -v b="$t" 'BEGIN{print (a==""||b<a)?b:a}')
			done
			local cx hx
			cx=$(awk -v n="$nc" -v i="$ic" 'BEGIN{printf (i>0)?"%.2f":"n/a", n/i}')
			hx=$(awk -v n="$nh" -v i="$ih" 'BEGIN{printf (i>0)?"%.2f":"n/a", n/i}')
			printf '%-22s %3s %12s %12s %7sx   %12s %12s %7sx\n' "$name" "$th" "$ic" "$nc" "$cx" "$ih" "$nh" "$hx"
		done
		shutdown
	done
	echo
	echo "x = native/iresearch ms (>1 means iresearch faster than native DuckDB; target >= 2.0)"
}

# Compare our branch vs the released serenedb docker on write (index build) and
# read (the query suite), plus on-disk .col size. Each engine runs isolated
# (never co-resident) under matched SENG_DOCKER/SENG_HOSTNET conditions.
cmd_compare() {
	[[ -f "$PARQUET" ]] || {
		echo "missing $PARQUET (sample_hits.sh)" >&2
		exit 1
	}
	local engines="${CS_ENGINES:-ours old}"
	local pq_dir
	pq_dir="$(dirname "$PARQUET")"
	declare -A W R SZ
	for eng in $engines; do
		local dd="${DATA}_cmp_${eng}"
		rm -rf "$dd"
		mkdir -p "$dd"
		SENG_NAME="cs_cmp_${eng}" seng_up "$eng" "$PORT" "$dd" "$pq_dir"
		local conn
		conn="$(seng_conn "$PORT")"
		local pq
		pq="$(printf '%s' "$PARQUET" | sed "s/'/''/g")"
		psql "$conn" -v ON_ERROR_STOP=1 -X \
			-c "CREATE TEXT SEARCH DICTIONARY perf_english(template='delimiter', delimiter=' ');" \
			-c "CREATE VIEW hits_view AS SELECT * FROM read_parquet('$pq');" >/dev/null
		local inc
		inc="$(psql "$conn" -At -v ON_ERROR_STOP=1 -X \
			-c "SELECT string_agg('\"'||column_name||'\"', ', ') FROM (DESCRIBE hits_view);")"
		[[ -n "$inc" ]] || {
			echo "empty INCLUDE list on $eng" >&2
			seng_down
			exit 1
		}
		W[$eng]="$(psql "$conn" -v ON_ERROR_STOP=1 -X -c '\timing on' \
			-c "CREATE INDEX hits_idx ON hits_view USING inverted(\"Title\" perf_english) INCLUDE ($inc);" 2>&1 |
			awk '/^Time: /{t=$2} END{printf "%s", t}')"
		local rsum=0
		for i in "${!Q_NAME[@]}"; do
			local sql="${Q_SQL[$i]//@T@/hits_idx}"
			local best=""
			for _ in $(seq 1 "$HOT_REPS"); do
				local t
				t="$(psql "$conn" -X -c "SET threads = 1;" -c '\timing on' -c "$sql" 2>&1 |
					awk '/^Time: /{x=$2} END{printf "%s", x}')"
				best="$(awk -v a="$best" -v b="$t" 'BEGIN{print (a==""||b<a)?b:a}')"
			done
			rsum="$(awk -v s="$rsum" -v b="$best" 'BEGIN{printf "%.3f", s+b}')"
		done
		R[$eng]="$rsum"
		seng_down
		SZ[$eng]="$(seng_datadir_bytes "$dd")"
	done
	echo
	printf '%-6s %16s %18s %18s\n' engine "index_build_ms" "read_suite_hot_ms" "col_datadir_bytes"
	printf '%-6s %16s %18s %18s\n' "------" "----------------" "------------------" "------------------"
	for eng in $engines; do
		printf '%-6s %16s %18s %18s\n' "$eng" "${W[$eng]:-n/a}" "${R[$eng]:-n/a}" "${SZ[$eng]:-n/a}"
	done
	echo
	echo "engines run in docker=${SENG_DOCKER:-1} hostnet=${SENG_HOSTNET:-1}; ours bin=${SENG_OURS_BIN}"
}

case "${1:-}" in
build) cmd_build ;;
bench) cmd_bench ;;
compare) cmd_compare ;;
*)
	echo "usage: $0 {build|bench|compare}" >&2
	exit 2
	;;
esac
