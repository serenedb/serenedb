#!/usr/bin/env bash
# col_codecs_matrix.sh -- size and speed matrix of the search-table (.col) string
# codecs on the ClickHouse hits columns.
#
#   for each arm (a codec name, optionally :level) x segment_target:
#     fresh serened + datadir, CREATE TABLE hits (id PK, <cols> USING COMPRESSION arm)
#       WITH (storage = 'search', compression_level, segment_target),
#     INSERT ... FROM read_parquet(hits.parquet) [LIMIT rows],
#     VACUUM (REFRESH_TABLE) [+ VACUUM (COMPACT_TABLE)],
#     bytes per column (pragma_storage_info byte_size), .col file bytes, datadir bytes, peak RSS,
#     reads per column, cold (after restart) and hot (min of CM_HOT):
#       scan            sum(length(col))
#       eq              count(*) WHERE col = <value of row 4242>
#       like            count(*) WHERE col LIKE '%<10 middle bytes of that value>%'
#       gather100k      sum(length(col)) WHERE id % 100000 = 7
#       gather1m        sum(length(col)) WHERE id % 1000000 = 7
#       points          CM_POINTS lookups WHERE id = k, summed
#
# Env: CM_BIN (build_bench/bin/serened), CM_PARQUET (~/data/hits.parquet),
#      CM_ROWS (0 = all), CM_ARMS, CM_TARGETS, CM_COLS, CM_PORT, CM_RES, CM_HOT,
#      CM_POINTS, CM_COMPACT (1), CM_KEEP (keep datadirs), CM_THREADS,
#      CM_OBJECTIVE (compression_objective of every table: balanced, size, speed),
#      CM_CACHE (sdb_object_cache_size in bytes after every start; 1 = no dictionary cache),
#      CM_PASSES (insert the parquet N times, ids = file_row_number + pass * CM_PASS_ROWS,
#                 refreshing after every pass so the WAL is reclaimed between passes),
#      CM_LEGACY (1 = a serened without the segment_target / compression_level options).
# Output: TSV under CM_RES plus a rendered table on stdout.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BIN="${CM_BIN:-${ROOT}/build_bench/bin/serened}"
PARQUET="${CM_PARQUET:-${HOME}/data/hits.parquet}"
ROWS="${CM_ROWS:-0}"
ARMS="${CM_ARMS:-auto dict_fsst dict_lz4 dict_zstd:1 dict_zxc:3 lz4 zxc:3 fsst zstd uncompressed}"
TARGETS="${CM_TARGETS:-262144}"
COLS="${CM_COLS:-URL Title Referer SearchPhrase}"
PORT="${CM_PORT:-6377}"
RES="${CM_RES:-${ROOT}/scripts/perf/results/col_codecs}"
HOT="${CM_HOT:-3}"
POINTS="${CM_POINTS:-200}"
COMPACT="${CM_COMPACT:-1}"
KEEP="${CM_KEEP:-0}"
THREADS="${CM_THREADS:-}"
OBJECTIVE="${CM_OBJECTIVE:-}"
PASSES="${CM_PASSES:-1}"
CACHE="${CM_CACHE:-}"
PASS_ROWS="${CM_PASS_ROWS:-100000000}"
LEGACY="${CM_LEGACY:-0}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${RES}"
OUT="${RES}/matrix-${STAMP}.tsv"
CONN="postgres://postgres@127.0.0.1:${PORT}/postgres"
SPID=""

up() {
	local datadir="$1"
	"${BIN}" "${datadir}" --listen="postgres://0.0.0.0:${PORT}" \
		>>"${RES}/serened-$(basename "${datadir}").log" 2>&1 &
	SPID=$!
	local _
	for _ in $(seq 1 600); do
		if psql "${CONN}" -X -c 'SELECT 1' >/dev/null 2>&1; then
			[[ -n "${CACHE}" ]] && psql "${CONN}" -X -c "SET GLOBAL sdb_object_cache_size = ${CACHE};" >/dev/null
			return 0
		fi
		kill -0 "${SPID}" 2>/dev/null || { echo "serened exited" >&2; return 1; }
		sleep 0.5
	done
	echo "serened on :${PORT} never became ready" >&2
	return 1
}
down() {
	[[ -z "${SPID}" ]] && return 0
	kill -9 "${SPID}" 2>/dev/null || true
	wait "${SPID}" 2>/dev/null || true
	SPID=""
}
trap down EXIT

psql_ms() { # sql... -> ms of the statements (sum of psql Time: lines), -1 on error
	local out
	if ! out=$(psql "${CONN}" -X -v ON_ERROR_STOP=1 -c '\timing on' "$@" 2>&1); then
		echo "SQL failed: $*" >&2
		echo "${out}" | tail -5 >&2
		echo -1
		return 1
	fi
	awk '/^Time: /{t+=$2} END{printf "%.1f\n", t}' <<<"${out}"
}
sql_at() { psql "${CONN}" -X -At -v ON_ERROR_STOP=1 -c "$1"; }
rss_mb() { awk '/VmHWM/{print int($2/1024)}' "/proc/${SPID}/status" 2>/dev/null || echo 0; }
row() { # arm target col metric value
	printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" | tee -a "${OUT}"
}
sqlq() { printf '%s' "$1" | sed "s/'/''/g"; }

read_sql() { # col metric -> sql
	local c="$1" m="$2"
	case "${m}" in
	scan) echo "SELECT sum(length(\"${c}\")) FROM hits;" ;;
	eq) echo "SELECT count(*) FROM hits WHERE \"${c}\" = '$(sqlq "${LIT[$c]}")';" ;;
	like) echo "SELECT count(*) FROM hits WHERE \"${c}\" LIKE '%$(sqlq "${MID[$c]}")%';" ;;
	gather100k) echo "SELECT sum(length(\"${c}\")) FROM hits WHERE id % 100000 = 7;" ;;
	gather1m) echo "SELECT sum(length(\"${c}\")) FROM hits WHERE id % 1000000 = 7;" ;;
	points)
		local k step sql=""
		step=$((NROWS / POINTS))
		[[ ${step} -lt 1 ]] && step=1
		for ((k = 1; k <= NROWS && k <= step * POINTS; k += step)); do
			sql+="SELECT \"${c}\" FROM hits WHERE id = ${k};"
		done
		echo "${sql}"
		;;
	esac
}

run_reads() { # phase(cold|hot) -> rows per col x metric
	local phase="$1" c m ms best i
	for c in ${COLS}; do
		for m in scan eq like gather100k gather1m points; do
			if [[ "${phase}" == cold ]]; then
				ms=$(psql_ms -c "$(read_sql "${c}" "${m}")")
			else
				best=""
				for ((i = 0; i < HOT; i++)); do
					ms=$(psql_ms -c "$(read_sql "${c}" "${m}")")
					[[ -z "${best}" ]] || awk "BEGIN{exit !(${ms} < ${best})}" && best="${ms}"
				done
				ms="${best}"
			fi
			row "${ARM}" "${TARGET}" "${c}" "${phase}_${m}_ms" "${ms}"
		done
	done
}

declare -A LIT MID
for TARGET in ${TARGETS}; do
	for ARMSPEC in ${ARMS}; do
		ARM="${ARMSPEC%%:*}"
		LEVEL=""
		[[ "${ARMSPEC}" == *:* ]] && LEVEL="${ARMSPEC##*:}"
		ARM="${ARMSPEC}${OBJECTIVE:+@${OBJECTIVE}}"
		codec="${ARMSPEC%%:*}"
		datadir="${RES}/data_${ARMSPEC//:/_}_${TARGET}"
		rm -rf "${datadir}"
		mkdir -p "${datadir}"
		echo "=== arm ${ARMSPEC} target ${TARGET}"
		up "${datadir}" || exit 1
		[[ -n "${THREADS}" ]] && psql "${CONN}" -X -c "SET GLOBAL threads = ${THREADS};" >/dev/null

		using=""
		[[ "${codec}" != auto ]] && using=" USING COMPRESSION ${codec}"
		cols_ddl=""
		sel=""
		for c in ${COLS}; do
			cols_ddl+=", \"${c}\" TEXT${using}"
			sel+=", \"${c}\""
		done
		with="storage = 'search', refresh_interval = 0, compaction_interval = 0"
		if [[ "${LEGACY}" != 1 ]]; then
			with+=", segment_target = ${TARGET}"
			[[ -n "${LEVEL}" ]] && with+=", compression_level = ${LEVEL}"
			[[ -n "${OBJECTIVE}" ]] && with+=", compression_objective = '${OBJECTIVE}'"
		fi
		limit=""
		[[ "${ROWS}" -gt 0 ]] && limit=" LIMIT ${ROWS}"
		psql "${CONN}" -X -v ON_ERROR_STOP=1 -c "CREATE TABLE hits (id BIGINT PRIMARY KEY${cols_ddl}) WITH (${with});" >/dev/null || exit 1

		if [[ "${PASSES}" -gt 1 ]]; then
			total=0
			for ((p = 0; p < PASSES; p++)); do
				ms=$(psql_ms -c "INSERT INTO hits SELECT file_row_number + ${p} * ${PASS_ROWS} AS id${sel} FROM read_parquet('${PARQUET}', file_row_number := true)${limit};" -c "VACUUM (REFRESH_TABLE) hits;") || exit 1
				total=$(awk "BEGIN{print ${total} + ${ms}}")
			done
			ms="${total}"
		else
			ms=$(psql_ms -c "INSERT INTO hits SELECT row_number() OVER () AS id${sel} FROM read_parquet('${PARQUET}')${limit};") || exit 1
		fi
		row "${ARM}" "${TARGET}" all insert_ms "${ms}"
		ms=$(psql_ms -c "VACUUM (REFRESH_TABLE) hits;") || exit 1
		row "${ARM}" "${TARGET}" all refresh_ms "${ms}"
		if [[ "${COMPACT}" == 1 ]]; then
			ms=$(psql_ms -c "VACUUM (COMPACT_TABLE) hits;") || exit 1
			row "${ARM}" "${TARGET}" all compact_ms "${ms}"
		fi
		row "${ARM}" "${TARGET}" all write_rss_mb "$(rss_mb)"
		NROWS=$(sql_at "SELECT count(*) FROM hits;")
		row "${ARM}" "${TARGET}" all rows "${NROWS}"
		for c in ${COLS}; do
			LIT[$c]=$(sql_at "SELECT \"${c}\" FROM hits WHERE id = 4242;")
			MID[$c]=$(sql_at "SELECT substr(\"${c}\", 3, 10) FROM hits WHERE id = 4242;")
		done
		while IFS=$'\t' read -r c comp segs bytes; do
			row "${ARM}" "${TARGET}" "${c}" "codec" "${comp}"
			row "${ARM}" "${TARGET}" "${c}" "segments" "${segs}"
			row "${ARM}" "${TARGET}" "${c}" "bytes" "${bytes}"
		done < <(psql "${CONN}" -X -At -F $'\t' -v ON_ERROR_STOP=1 -c "
SELECT column_name, string_agg(DISTINCT compression, ','), count(*),
       sum(TRY_CAST(regexp_extract(segment_info, 'byte_size=(\\d+)', 1) AS BIGINT))
FROM pragma_storage_info('hits', include_segment_info := true)
WHERE segment_type <> 'VALIDITY' AND column_name <> 'id'
GROUP BY column_name ORDER BY column_name;")
		row "${ARM}" "${TARGET}" all datadir_bytes "$(du -sb "${datadir}" | awk '{print $1}')"
		row "${ARM}" "${TARGET}" all col_bytes "$(find "${datadir}" -name '*.col' -printf '%s\n' | awk '{s+=$1} END {print s+0}')"

		down
		up "${datadir}" || exit 1
		run_reads cold
		run_reads hot
		row "${ARM}" "${TARGET}" all read_rss_mb "$(rss_mb)"
		down
		[[ "${KEEP}" == 1 ]] || rm -rf "${datadir}"
	done
done

echo
echo "raw TSV: ${OUT}"
python3 - "${OUT}" <<'EOF'
import sys, collections
rows = [l.rstrip("\n").split("\t") for l in open(sys.argv[1])]
d = collections.defaultdict(dict)
for arm, target, col, metric, value in rows:
    d[(arm, target, col)][metric] = value
arms = sorted({(a, t) for a, t, _ in d})
cols = sorted({c for _, _, c in d if c != "all"})
def num(v):
    try:
        return float(v)
    except Exception:
        return None
print("\n== bytes per column (MiB) and write times (s)")
hdr = f"{'arm':14} {'target':>7} " + " ".join(f"{c:>13}" for c in cols) + f" {'total':>9} {'.col':>9} {'insert':>8} {'refresh':>8} {'compact':>8} {'rss':>6}"
print(hdr)
for a, t in arms:
    cells = []
    tot = 0
    for c in cols:
        b = num(d[(a, t, c)].get("bytes"))
        tot += b or 0
        cells.append(f"{(b or 0)/2**20:13.1f}")
    al = d[(a, t, "all")]
    colb = num(al.get("col_bytes")) or 0
    ins = num(al.get("insert_ms")) or 0
    ref = num(al.get("refresh_ms")) or 0
    cmp_ = num(al.get("compact_ms")) or 0
    print(f"{a:14} {t:>7} " + " ".join(cells) + f" {tot/2**20:9.1f} {colb/2**20:9.1f} {ins/1000:8.1f} {ref/1000:8.1f} {cmp_/1000:8.1f} {al.get('write_rss_mb','-'):>6}")
for phase in ("cold", "hot"):
    for m in ("scan", "eq", "like", "gather100k", "gather1m", "points"):
        print(f"\n== {phase} {m} (ms)")
        print(f"{'arm':14} {'target':>7} " + " ".join(f"{c:>13}" for c in cols))
        for a, t in arms:
            cells = []
            for c in cols:
                v = num(d[(a, t, c)].get(f"{phase}_{m}_ms"))
                cells.append(f"{v:13.1f}" if v is not None else f"{'-':>13}")
            print(f"{a:14} {t:>7} " + " ".join(cells))
EOF
