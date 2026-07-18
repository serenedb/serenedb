#!/usr/bin/env bash
# bench_recovery_large_wal.sh -- recovery-speed A/B: OLD rocksdb table plane
# (pinned main baseline) vs NEW native duckdb store tables (branch perf binary),
# under a LARGE inverted-indexed dataset with a controlled post-checkpoint WAL
# delta. This is the metric the no-rebuild recovery design exists for: recovery
# cost should track the WAL delta (O(delta)), NOT the table size (O(table)).
#
# Per binary, on its own datadir (the two storage formats are not
# interchangeable):
#   1. build a large inverted-indexed text table (TABLE_ROWS),
#   2. CHECKPOINT so the bulk is durable in the checkpoint, not the WAL,
#   3. disable auto-checkpoint, then append DELTA_ROWS -- the "large WAL" that a
#      crash leaves un-checkpointed,
#   4. kill -9, then wall-clock restart-to-search-ready (a search query must
#      return the pre-crash hit count, so the index delta really is applied),
#   5. repeat REPS times on the same datadir (replay is idempotent; the WAL is
#      never truncated because auto-checkpoint stays off and kill -9 skips the
#      clean-shutdown checkpoint), report the median.
#
# Pre-reqs: psql, the baseline binary, the branch perf binary. No other serened
# on PORT.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
NEW_BIN="${PERF_STORAGE_NEW_BIN:-${BUILD_DIR}/bin/serened}"
OLD_BIN="${PERF_STORAGE_OLD_BIN:-${HOME}/baselines/serened-main-aa1595ddb}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${RESULTS_DIR}/recovery-large-wal-${STAMP}"

PORT="${PERF_REC_PORT:-6491}"
TABLE_ROWS="${PERF_REC_TABLE_ROWS:-2000000}"
DELTA_ROWS="${PERF_REC_DELTA_ROWS:-300000}"
REPS="${PERF_REC_REPS:-3}"
IO_THREADS="${PERF_REC_IO_THREADS:-4}"
CPU_THREADS="${PERF_REC_CPU_THREADS:-8}"
SERVER_CORES="${PERF_REC_SERVER_CORES:-8-15}"
QUIET_LOAD="${PERF_QUIET_LOAD:-8}"

for bin in "${OLD_BIN}" "${NEW_BIN}"; do
	[[ -x "${bin}" ]] || {
		echo "missing binary: ${bin}" >&2
		exit 1
	}
done
command -v psql >/dev/null 2>&1 || {
	echo "psql not found" >&2
	exit 1
}

mkdir -p "${OUT_DIR}"
PSQL=(psql -h 127.0.0.1 -p "${PORT}" -U postgres -d postgres -AtX -q)
CUR_PID=""
CUR_DATA=""
cleanup() {
	[[ -n "${CUR_PID}" ]] && kill -9 "${CUR_PID}" 2>/dev/null || true
	[[ -n "${CUR_DATA}" ]] && rm -rf "${CUR_DATA}" || true
}
trap cleanup EXIT

wait_quiet() {
	local waited=0 load
	while :; do
		load="$(awk '{print int($1)}' /proc/loadavg)"
		((load <= QUIET_LOAD)) && return 0
		((waited >= 600)) && {
			echo "  WARNING: load ${load} -- measuring anyway" >&2
			return 0
		}
		sleep 10
		waited=$((waited + 10))
	done
}

start_serened() {
	local srv="$1" datadir="$2"
	taskset -c "${SERVER_CORES}" "${BENCH_BIN}" "${datadir}" \
		--listen "postgres://0.0.0.0:${PORT}" \
		--io_threads "${IO_THREADS}" \
		--cpu_threads "${CPU_THREADS}" \
		>>"${OUT_DIR}/serened_${srv}.log" 2>&1 &
	CUR_PID=$!
}

wait_up() {
	for _ in $(seq 1 240); do
		"${PSQL[@]}" -c 'SELECT 1' >/dev/null 2>&1 && return 0
		sleep 0.25
	done
	echo "server on :${PORT} did not come up" >&2
	return 1
}

# Block until a search over the indexed table returns the expected hit count,
# i.e. the post-checkpoint delta is fully applied to the inverted index.
wait_search_ready() {
	local want="$1"
	for _ in $(seq 1 4000); do
		local got
		got="$("${PSQL[@]}" -c "SELECT count(*) FROM rec_txt_idx WHERE body @@ ts_phrase('token7');" 2>/dev/null || echo -1)"
		[[ "${got}" == "${want}" ]] && return 0
		sleep 0.05
	done
	echo "search never reached ${want} hits" >&2
	return 1
}

wal_bytes() { du -sb "${1}" 2>/dev/null | awk '{print $1}'; }

bench_one() {
	local srv="$1"
	local datadir
	datadir="$(mktemp -d "/tmp/sdb_rec_${srv}.XXXXXX")"
	CUR_DATA="${datadir}"
	echo "-- ${srv}: build TABLE_ROWS=${TABLE_ROWS} + DELTA_ROWS=${DELTA_ROWS} (cores ${SERVER_CORES})"
	start_serened "${srv}" "${datadir}"
	wait_up

	"${PSQL[@]}" \
		-c "CREATE TEXT SEARCH DICTIONARY rec_dict(template='text', locale='en_US.UTF-8', case='none', stemming=false, accent=false, frequency=true, position=true);" \
		-c "CREATE TABLE rec_txt (id INTEGER PRIMARY KEY, body TEXT);" \
		-c "INSERT INTO rec_txt SELECT x, 'lorem ipsum dolor sit amet word' || (x % 1000) || ' token' || (x % 50) FROM generate_series(1, ${TABLE_ROWS}) t(x);" \
		-c "CREATE INDEX rec_txt_idx ON rec_txt USING inverted(body rec_dict);" \
		-c "VACUUM (REFRESH_TABLE) rec_txt;" \
		-c "CHECKPOINT;" >/dev/null

	# Disable auto-checkpoint, then append the delta so it lands in the WAL and
	# stays there (tolerate the old binary lacking these settings).
	"${PSQL[@]}" \
		-c "SET GLOBAL wal_autocheckpoint='1TB';" \
		-c "SET GLOBAL checkpoint_threshold='1TB';" 2>/dev/null || true
	"${PSQL[@]}" \
		-c "INSERT INTO rec_txt SELECT x, 'lorem ipsum dolor sit amet word' || (x % 1000) || ' token' || (x % 50) FROM generate_series(${TABLE_ROWS} + 1, ${TABLE_ROWS} + ${DELTA_ROWS}) t(x);" \
		-c "VACUUM (REFRESH_TABLE) rec_txt;" >/dev/null

	local total=$((TABLE_ROWS + DELTA_ROWS))
	local hits
	hits="$("${PSQL[@]}" -c "SELECT count(*) FROM rec_txt_idx WHERE body @@ ts_phrase('token7');")"
	local wal
	wal="$(wal_bytes "${datadir}")"
	echo "   rows=${total} token7_hits=${hits} datadir_bytes=${wal}"

	local times=() rss_list=() r s e
	for ((r = 0; r < REPS; r++)); do
		wait_quiet
		kill -9 "${CUR_PID}" 2>/dev/null || true
		wait "${CUR_PID}" 2>/dev/null || true
		s=$(date +%s.%N)
		start_serened "${srv}" "${datadir}"
		wait_up
		wait_search_ready "${hits}"
		e=$(date +%s.%N)
		local dt rss
		dt="$(echo "${e} - ${s}" | bc)"
		# Peak RSS of the freshly recovered process: what replay itself costs
		# in memory before any query traffic.
		rss="$(awk '/VmHWM/{print $2}' "/proc/${CUR_PID}/status" 2>/dev/null || echo 0)"
		times+=("${dt}")
		rss_list+=("${rss}")
		printf '   rep %d: %ss rss=%sKB\n' "${r}" "${dt}" "${rss}"
	done
	kill -9 "${CUR_PID}" 2>/dev/null || true
	wait "${CUR_PID}" 2>/dev/null || true
	CUR_PID=""

	local med med_rss
	med=$(printf '%s\n' "${times[@]}" | sort -n | awk -v n="${REPS}" 'NR==int((n+1)/2){print}')
	med_rss=$(printf '%s\n' "${rss_list[@]}" | sort -n | awk -v n="${REPS}" 'NR==int((n+1)/2){print}')
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "${srv}" "${total}" "${DELTA_ROWS}" "${hits}" "${wal}" "${med}" "${med_rss}" >>"${OUT_DIR}/results.tsv"
	echo "   ${srv} recovery median=${med}s rss=${med_rss}KB"
	rm -rf "${datadir}"
	CUR_DATA=""
}

: >"${OUT_DIR}/results.tsv"
echo "old: ${OLD_BIN}"
echo "new: ${NEW_BIN}"
echo "out: ${OUT_DIR}"
echo "table_rows=${TABLE_ROWS} delta_rows=${DELTA_ROWS} reps=${REPS}"
echo

BENCH_BIN="${OLD_BIN}" bench_one old
BENCH_BIN="${NEW_BIN}" bench_one new

echo
echo "============ RECOVERY (LARGE WAL) SUMMARY ============"
awk -F'\t' '
{ med[$1]=$6; rows[$1]=$2; delta[$1]=$3; wal[$1]=$5; rss[$1]=$7 }
END {
  printf "%-6s %12s %12s %14s %12s %12s\n","side","rows","delta","datadir_bytes","recovery_s","peak_rss_kb";
  printf "%-6s %12s %12s %14s %12s %12s\n","----","------------","------------","--------------","------------","------------";
  for (s in med) printf "%-6s %12s %12s %14s %12s %12s\n", s, rows[s], delta[s], wal[s], med[s], rss[s];
  if ((med["old"]+0)>0 && (med["new"]+0)>0)
    printf "\nspeedup new vs old: %.1fx\n", med["old"]/med["new"];
}' "${OUT_DIR}/results.tsv" | tee "${OUT_DIR}/summary.txt"
echo "====================================================="
echo "results: ${OUT_DIR}"
