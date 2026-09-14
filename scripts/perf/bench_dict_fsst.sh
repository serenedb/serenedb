#!/usr/bin/env bash
# bench_dict_fsst.sh -- dict_fsst string-compression bench on ONE binary:
# on-disk size, write cost (CTAS + CHECKPOINT) and read cost (full scan +
# constant filter) per string shape, swept over force_dict_fsst_mode.
#
# Unlike the other scripts here the axis is NOT two binaries or two engines:
# it is the compression mode on a single serened. To compare code revisions,
# run it, rebuild, run it again, then re-run with DF_BASELINE=<the first TSV>
# for a delta table. Each result file is identified by its timestamp only --
# what a given build contained is the runner's to track.
#
# force_compression='dict_fsst' pins the codec so we always measure this
# compressor, and force_dict_fsst_mode picks the layout family. Both are
# GLOBAL_ONLY settings; cells run sequentially so that is safe.
#
# Each (shape, mode, rep) gets its OWN attached database file. That is not
# tidiness: a failed CHECKPOINT invalidates the attached database it happened
# in, so per-cell files are what let a build that still overflows finish the
# sweep and report FAILED for exactly the cells that threw.
#
# Shapes (string regimes, each hitting a different layout):
#   urls         prefix-heavy unique     -> cleaved (DICT_FSST_PLUS / FSST_PLUS)
#   logs         templated log lines     -> cleaved, shorter prefixes
#   tokens       random hex, unique      -> FSST_ONLY (nothing to share)
#   lowcard      143 values in runs      -> DICTIONARY / DICT_FSST
#   pathological the prefix-id-width shape from
#                tests/sqllogic/recovery/dict_fsst_cut_prefix_id_width.test
#
# Env:
#   DF_BIN       serened binary            (build/bin/serened; DEBUG is fine
#                                           for size, noisy for timings)
#   DF_LABEL     extra result-file tag     (none)
#   DF_PORT      port to own               (6265)
#   DF_ROWS      rows per shape            (300000; pathological is pinned).
#                Size is measured in 256KB blocks, so keep this high enough
#                that quantisation does not swamp the difference -- at 300k
#                rows a shape occupies ~20 blocks, i.e. ~5% granularity.
#   DF_REPS      write reps, median taken  (3)
#   DF_SHAPES    shape subset              (all of the above)

#   DF_COLD=1    restart serened before the reads (dr#   DF_MODES     force_dict_fsst_mode list (DEFAULT AUTO_NATIVE)ops the buffer pool, so
#                the scan re-reads and re-decompresses; no sudo needed)
#   DF_BASELINE  previous TSV to diff against
#   DF_KEEP=1    keep the per-cell .db files for inspection

set -uo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BIN="${DF_BIN:-${ROOT}/build/bin/serened}"
PORT="${DF_PORT:-6265}"
ROWS="${DF_ROWS:-300000}"
REPS="${DF_REPS:-3}"
SHAPES="${DF_SHAPES:-urls logs tokens lowcard pathological}"
MODES="${DF_MODES:-DEFAULT AUTO_NATIVE}"
COLD="${DF_COLD:-0}"
KEEP="${DF_KEEP:-0}"
BASELINE="${DF_BASELINE:-}"
LABEL="${DF_LABEL:-}"

RESULTS_DIR="${ROOT}/scripts/perf/results"
WORK_DIR="${RESULTS_DIR}/dict_fsst_work"
DATA_DIR="${WORK_DIR}/serened_data"
SLOG="${WORK_DIR}/serened.log"
CONN="postgres://postgres@127.0.0.1:${PORT}/postgres"

if [[ ! -x "${BIN}" ]]; then
	echo "missing ${BIN} -- build it first, or set DF_BIN=..." >&2
	exit 2
fi

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
if [[ -n "${LABEL}" ]]; then
	TSV="${RESULTS_DIR}/dict_fsst-${STAMP}-${LABEL}.tsv"
else
	TSV="${RESULTS_DIR}/dict_fsst-${STAMP}.tsv"
fi
BIN_MTIME=$(stat -c%y "${BIN}" | cut -d. -f1)

mkdir -p "${RESULTS_DIR}" "${WORK_DIR}"

SERENED_PID=""
cleanup() {
	if [[ -n "${SERENED_PID}" ]] && kill -0 "${SERENED_PID}" 2>/dev/null; then
		kill -9 "${SERENED_PID}" 2>/dev/null
		wait "${SERENED_PID}" 2>/dev/null
	fi
	if [[ "${KEEP}" != "1" ]]; then
		rm -rf "${WORK_DIR}"
	fi
}
trap cleanup EXIT INT TERM

start_serened() {
	rm -rf "${DATA_DIR}"
	mkdir -p "${DATA_DIR}"
	"${BIN}" "${DATA_DIR}" --listen="postgres://0.0.0.0:${PORT}" >"${SLOG}" 2>&1 &
	SERENED_PID=$!
	local i
	for i in $(seq 1 240); do
		if psql "${CONN}" -X -q -tAc 'SELECT 1' >/dev/null 2>&1; then
			return 0
		fi
		if ! kill -0 "${SERENED_PID}" 2>/dev/null; then
			echo "serened died on startup -- see ${SLOG}" >&2
			tail -20 "${SLOG}" >&2
			exit 1
		fi
		sleep 0.5
	done
	echo "serened did not come up on ${PORT} -- see ${SLOG}" >&2
	exit 1
}

stop_serened() {
	if [[ -n "${SERENED_PID}" ]] && kill -0 "${SERENED_PID}" 2>/dev/null; then
		kill -9 "${SERENED_PID}" 2>/dev/null
		wait "${SERENED_PID}" 2>/dev/null
	fi
	SERENED_PID=""
}

# Pull the LAST `Time: <ms> ms ...` psql line, in fractional ms as printed.
extract_last_time_ms() {
	awk '/^Time: /{t=$2} END{if (t!="") printf "%s", t}' <<<"$1"
}

# Run one timed statement. Sets SQL_OUT, SQL_RC and SQL_MS.
timed_sql() {
	local sql="$1"
	SQL_OUT=$(psql "${CONN}" -X -q -v ON_ERROR_STOP=1 \
		-c '\timing on' \
		-c "${sql}" 2>&1)
	SQL_RC=$?
	SQL_MS=$(extract_last_time_ms "${SQL_OUT}")
	[[ -n "${SQL_MS}" ]] || SQL_MS="NA"
}

plain_sql() {
	psql "${CONN}" -X -q -v ON_ERROR_STOP=1 -tAc "$1" 2>&1
}

median() {
	local vals n
	vals=$(printf '%s\n' "$@" | grep -v '^NA$' | sort -g)
	n=$(printf '%s\n' "${vals}" | grep -c .)
	if [[ "${n}" -eq 0 ]]; then
		printf 'NA'
		return
	fi
	printf '%s\n' "${vals}" | awk -v n="${n}" 'NR==int((n+1)/2){printf "%s", $0}'
}

mean2() {
	awk -v a="$1" -v b="$2" 'BEGIN{
		if (a=="NA" || b=="NA") { printf "NA"; exit }
		printf "%.3f", (a+b)/2
	}'
}

shape_rows() {
	case "$1" in
	pathological) printf '43750' ;;
	*) printf '%s' "${ROWS}" ;;
	esac
}

# The per-shape value expression over `ord`. Quoted heredocs: SQL quotes pass
# through untouched.
shape_expr() {
	case "$1" in
	urls)
		cat <<'SQL'
'https://cdn.example.com/assets/tenant-' || lpad((hash(ord) % 50)::VARCHAR, 2, '0')
  || '/images/' || lpad(ord::VARCHAR, 9, '0') || '.jpg?v=' || substr(md5(ord::VARCHAR), 1, 12)
SQL
		;;
	logs)
		cat <<'SQL'
'2026-09-03 ' || lpad(((ord // 3600) % 24)::VARCHAR, 2, '0') || ':'
  || lpad(((ord // 60) % 60)::VARCHAR, 2, '0') || ':' || lpad((ord % 60)::VARCHAR, 2, '0')
  || CASE (hash(ord) % 4)
       WHEN 0 THEN ' INFO  request served path=/api/v1/items/'
       WHEN 1 THEN ' WARN  slow query on table orders id='
       WHEN 2 THEN ' INFO  cache miss key=user:'
       ELSE ' ERROR upstream timeout host=db-' END
  || (hash(ord * 7) % 100000)::VARCHAR
SQL
		;;
	tokens)
		cat <<'SQL'
substr(md5(ord::VARCHAR) || md5((ord * 7919)::VARCHAR) || md5((ord * 104729)::VARCHAR),
       1, (40 + (hash(ord) % 61))::INTEGER)
SQL
		;;
	lowcard)
		cat <<'SQL'
'k' || lpad((ord // 2100)::VARCHAR, 3, '0') || repeat('x', 36)
  || substr(md5((ord // 2100)::VARCHAR), 1, 8)
SQL
		;;
	pathological)
		cat <<'SQL'
CASE WHEN ord < 5200 THEN
  chr((65 + ord // 2600)::INTEGER) || 'QWERTYUIOPASDFGHJKLZXCVBNM0123456789QWERTYUIOPA'
  || chr((48 + (ord % 2600) % 3)::INTEGER)
  || chr((32 + (((ord % 2600) * 7919) % 857375) % 95)::INTEGER)
  || chr((32 + ((((ord % 2600) * 7919) % 857375) // 95) % 95)::INTEGER)
  || chr((32 + ((((ord % 2600) * 7919) % 857375) // 9025) % 95)::INTEGER)
  || chr((32 + (((ord % 2600) * 104729) % 857375) % 95)::INTEGER)
  || chr((32 + ((((ord % 2600) * 104729) % 857375) // 95) % 95)::INTEGER)
  || chr((32 + ((((ord % 2600) * 104729) % 857375) // 9025) % 95)::INTEGER)
  || chr((32 + ((ord % 2600) * 31) % 95)::INTEGER)
WHEN ord < 34000 THEN
  chr((32 + (ord - 5200) % 95)::INTEGER)
  || chr((32 + ((((ord - 5200) // 95) * 7919) % 857375) % 95)::INTEGER)
  || chr((32 + (((((ord - 5200) // 95) * 7919) % 857375) // 95) % 95)::INTEGER)
  || chr((32 + (((((ord - 5200) // 95) * 7919) % 857375) // 9025) % 95)::INTEGER)
ELSE
  chr((97 + ((ord - 34000) // 2) % 26)::INTEGER)
  || chr((97 + (((ord - 34000) // 2) // 26) % 26)::INTEGER)
  || substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ',
            (2 * (((ord - 34000) // 2) // 676) + ((ord - 34000) % 2) + 1)::INTEGER, 1)
END
SQL
		;;
	*)
		echo "unknown shape: $1" >&2
		exit 2
		;;
	esac
}

{
	printf '# dict_fsst bench\n'
	printf '# started\t%s\n' "${STAMP}"
	printf '# binary\t%s\n' "${BIN}"
	printf '# binary_mtime\t%s\n' "${BIN_MTIME}"
	printf '# rows\t%s\n' "${ROWS}"
	printf '# reps\t%s\n' "${REPS}"
	printf '# cold\t%s\n' "${COLD}"
	printf '# nproc\t%s\n' "$(nproc)"
	printf '# load_start\t%s\n' "$(cut -d' ' -f1-3 /proc/loadavg)"
	printf 'shape\tmode\trows\tstatus\tbytes\tblocks\tblock_size\tfile_bytes\tsegments\tseg_modes\tctas_ms\tckpt_ms\tscan_ms\tfilt_ms\n'
} >"${TSV}"

echo "=== dict_fsst bench: rows=${ROWS} reps=${REPS} cold=${COLD}"
echo "=== binary: ${BIN} (${BIN_MTIME})"
echo "=== results: ${TSV}"

start_serened

for shape in ${SHAPES}; do
	expr_sql=$(shape_expr "${shape}")
	srows=$(shape_rows "${shape}")
	mid=$((srows / 2))
	for mode in ${MODES}; do
		printf '\n--- %s/%s (rows=%s) ---\n' "${shape}" "${mode}" "${srows}"
		# The alias carries the mode: a failed CHECKPOINT can leave its
		# database attached, and reusing the name would then collide.
		mode_tag=$(tr 'A-Z' 'a-z' <<<"${mode}")

		status="ok"
		ctas_list=()
		ckpt_list=()
		bytes="NA" blocks="NA" bsize="NA" file_bytes="NA"
		segments="NA" seg_modes="NA" scan_ms="NA" filt_ms="NA"
		alias="" db_path=""

		for rep in $(seq 1 "${REPS}"); do
			alias="df_${shape}_${mode_tag}_${rep}"
			db_path="${WORK_DIR}/${alias}.db"
			rm -f "${db_path}" "${db_path}.wal"

			if ! psql "${CONN}" -X -q -v ON_ERROR_STOP=1 \
				-c "SET force_compression='dict_fsst';" \
				-c "SET force_dict_fsst_mode='${mode}';" \
				-c "ATTACH '${db_path}' AS ${alias} (TYPE duckdb, STORAGE_VERSION 'serenedb_v1');" \
				>/dev/null 2>&1; then
				status="attach_failed"
				break
			fi

			timed_sql "CREATE TABLE ${alias}.t AS SELECT ord, ${expr_sql} AS s FROM range(${srows}) t(ord) ORDER BY ord;"
			if [[ ${SQL_RC} -ne 0 ]]; then
				status="ctas_failed"
				printf '%s\n' "${SQL_OUT}" | grep -Eo 'ERROR:.*' | head -1
				break
			fi
			ctas_list+=("${SQL_MS}")
			printf '  rep%s ctas %s ms' "${rep}" "${SQL_MS}"

			timed_sql "CHECKPOINT ${alias};"
			if [[ ${SQL_RC} -ne 0 ]]; then
				status="ckpt_failed"
				printf '\n'
				printf '%s\n' "${SQL_OUT}" | grep -Eo 'ERROR:.*' | head -1
				break
			fi
			ckpt_list+=("${SQL_MS}")
			printf ' | ckpt %s ms\n' "${SQL_MS}"

			[[ "${rep}" -eq "${REPS}" ]] && break
			psql "${CONN}" -X -q -c "DETACH ${alias};" >/dev/null 2>&1
			rm -f "${db_path}" "${db_path}.wal"
		done

		if [[ "${status}" == "ok" ]]; then
			read -r blocks bsize <<<"$(plain_sql "SELECT used_blocks, block_size FROM pragma_database_size() WHERE database_name='${alias}';" | tr '|' ' ')"
			if [[ "${blocks}" =~ ^[0-9]+$ && "${bsize}" =~ ^[0-9]+$ ]]; then
				bytes=$((blocks * bsize))
			fi
			segments=$(plain_sql "SELECT count(*) FROM pragma_storage_info('${alias}.t') WHERE column_name='s' AND segment_type='VARCHAR';")
			# ORDER BY keeps the list stable across rounds, so averaging
			# does not report a spurious "mixed".
			seg_modes=$(plain_sql "SELECT string_agg(m, ',' ORDER BY m) FROM (SELECT DISTINCT replace(segment_info, 'mode: ', '') AS m FROM pragma_storage_info('${alias}.t', include_segment_info=true) WHERE column_name='s' AND segment_type='VARCHAR');")
			[[ -n "${seg_modes}" ]] || seg_modes="none"

			if [[ "${COLD}" == "1" ]]; then
				psql "${CONN}" -X -q -c "DETACH ${alias};" >/dev/null 2>&1
				stop_serened
				start_serened
				psql "${CONN}" -X -q -v ON_ERROR_STOP=1 \
					-c "ATTACH '${db_path}' AS ${alias} (TYPE duckdb);" >/dev/null 2>&1
			fi

			# Read path 1: full scan materialising every value.
			launches=()
			for k in 1 2 3; do
				timed_sql "SELECT count(*), sum(length(s)) FROM ${alias}.t;"
				[[ ${SQL_RC} -eq 0 ]] || break
				launches+=("${SQL_MS}")
			done
			if [[ "${COLD}" == "1" ]]; then
				scan_ms="${launches[0]:-NA}"
			else
				scan_ms=$(mean2 "${launches[1]:-NA}" "${launches[2]:-NA}")
			fi
			printf '  scan %s ms (launches: %s)\n' "${scan_ms}" "${launches[*]:-none}"

			# Read path 2: constant equality filter over the compressed column.
			probe=$(plain_sql "SELECT s FROM ${alias}.t WHERE ord = ${mid};")
			probe_sql=${probe//\'/\'\'}
			flaunch=()
			for k in 1 2 3; do
				timed_sql "SELECT count(*) FROM ${alias}.t WHERE s = '${probe_sql}';"
				[[ ${SQL_RC} -eq 0 ]] || break
				flaunch+=("${SQL_MS}")
			done
			filt_ms=$(mean2 "${flaunch[1]:-NA}" "${flaunch[2]:-NA}")
			printf '  filter %s ms\n' "${filt_ms}"

			psql "${CONN}" -X -q -c "CHECKPOINT ${alias};" >/dev/null 2>&1
			psql "${CONN}" -X -q -c "DETACH ${alias};" >/dev/null 2>&1
			[[ -f "${db_path}" ]] && file_bytes=$(du -sb "${db_path}" | awk '{print $1}')
		else
			psql "${CONN}" -X -q -c "DETACH ${alias};" >/dev/null 2>&1
			echo "  status: ${status}"
		fi

		[[ "${KEEP}" == "1" ]] || rm -f "${db_path}" "${db_path}.wal"

		printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
			"${shape}" "${mode}" "${srows}" "${status}" \
			"${bytes}" "${blocks}" "${bsize}" "${file_bytes}" \
			"${segments}" "${seg_modes}" \
			"$(median "${ctas_list[@]:-NA}")" "$(median "${ckpt_list[@]:-NA}")" \
			"${scan_ms}" "${filt_ms}" >>"${TSV}"
	done
done

stop_serened

# Timing columns are only as good as the box was quiet; both loadavgs are in
# the header so a round can be judged, or discarded, after the fact.
printf '# load_end\t%s\n' "$(cut -d' ' -f1-3 /proc/loadavg)" >>"${TSV}"

echo
echo "=== summary (TSV keeps raw bytes) ==="
awk -F'\t' '
# split() needs an explicit separator here: FS is a tab.
function human(b) {
	if (b == "NA" || b == "") return "NA"
	split("B KB MB GB TB", u, " ")
	i = 1
	while (b >= 1024 && i < 5) { b /= 1024; i++ }
	return sprintf("%.2f %s", b, u[i])
}
BEGIN{
	printf "%-13s %-12s %-13s %10s %10s %5s %-22s %10s %10s %10s %10s\n",
	       "shape","mode","status","size","file","segs","modes",
	       "ctas_ms","ckpt_ms","scan_ms","filt_ms"
}
/^#/ || /^shape\t/ {next}
{
	printf "%-13s %-12s %-13s %10s %10s %5s %-22s %10s %10s %10s %10s\n",
	       $1,$2,$4,human($5),human($8),$9,$10,$11,$12,$13,$14
}' "${TSV}"

if [[ -n "${BASELINE}" ]]; then
	if [[ ! -f "${BASELINE}" ]]; then
		echo "DF_BASELINE=${BASELINE} not found" >&2
		exit 1
	fi
	echo
	echo "=== delta vs $(basename "${BASELINE}") (negative = smaller/faster now) ==="
	awk -F'\t' -v basefile="${BASELINE}" '
	function pct(new, old) {
		if (old == "NA" || new == "NA" || old + 0 == 0) return "     n/a"
		return sprintf("%+7.1f%%", (new - old) / old * 100)
	}
	function human(b) {
		if (b == "NA" || b == "") return "NA"
		split("B KB MB GB TB", u, " ")
		i = 1
		while (b >= 1024 && i < 5) { b /= 1024; i++ }
		return sprintf("%.2f %s", b, u[i])
	}
	BEGIN {
		while ((getline line < basefile) > 0) {
			if (line ~ /^#/ || line ~ /^shape\t/) continue
			if (split(line, f, "\t") < 14) continue
			k = f[1] "/" f[2]
			bstat[k] = f[4]; bbytes[k] = f[5]; bctas[k] = f[11]
			bckpt[k] = f[12]; bscan[k] = f[13]; bfilt[k] = f[14]
			seen[k] = 1
		}
		printf "%-13s %-12s %-24s %10s %10s %8s %8s %8s %8s %8s\n",
		       "shape","mode","status","size_old","size_new","size","ctas","ckpt","scan","filt"
	}
	/^#/ || /^shape\t/ {next}
	{
		k = $1 "/" $2
		st = (k in seen) ? (bstat[k] == $4 ? $4 : bstat[k] "->" $4) : "(new)"
		printf "%-13s %-12s %-24s %10s %10s %8s %8s %8s %8s %8s\n",
		       $1, $2, st, human(bbytes[k]), human($5),
		       pct($5, bbytes[k]), pct($11, bctas[k]), pct($12, bckpt[k]),
		       pct($13, bscan[k]), pct($14, bfilt[k])
	}' "${TSV}"
fi

echo
echo "TSV: ${TSV}"
