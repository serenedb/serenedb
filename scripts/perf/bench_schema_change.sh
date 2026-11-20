#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
OLD_BIN="${SC_OLD_BIN:-${ROOT}/../serenedb-baseline/build_perf/bin/serened}"
NEW_BIN="${SC_NEW_BIN:-${ROOT}/build_perf/bin/serened}"
ROWS="${SC_ROWS:-10000000}"
REPS="${SC_REPS:-3}"
PORT="${SC_PORT:-6511}"
MAX_LOAD="${SC_MAX_LOAD:-4}"
FORCE="${FORCE:-0}"
DATA_ROOT="${SC_DATA_ROOT:-/tmp}"
SETTLE_SECONDS="${SC_SETTLE_SECONDS:-600}"
read -ra SERVER_ARGS <<<"${SC_SERVER_ARGS:-}"
ALL_CASES="create_index_text create_index_verbatim create_index_second add_column_null add_column_const add_column_volatile alter_type add_unique add_pk"
CASES="${SC_CASES:-${ALL_CASES}}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${RESULTS_DIR}/schema-change-${STAMP}.tsv"

usage() {
	cat <<EOF
Times schema changes on a populated search table: old binary against new.

Each round builds the table once per binary, waits for compaction to settle,
and runs every case on a fresh copy of that data directory.

Cases: ${ALL_CASES}

Per case it records the statement's response time, the bytes the server wrote
(/proc/<pid>/io write_bytes), the bytes appended to the WAL, and the table's
store size, file count and segment count right after the statement.

Environment:
  SC_OLD_BIN        baseline binary         (${OLD_BIN})
  SC_NEW_BIN        binary under test       (${NEW_BIN})
  SC_ENGINES        any number of "name:binary" pairs, replacing the two above
                    (e.g. "old:/a/serened fix1:/b/serened fix13:/c/serened")
  SC_ROWS           rows in the table       (${ROWS})
  SC_REPS           rounds                  (${REPS})
  SC_CASES          cases to run            (all)
  SC_PORT           port                    (${PORT})
  SC_DATA_ROOT      where data dirs go      (${DATA_ROOT})
  SC_MAX_LOAD       refuse above this 1-minute load average (${MAX_LOAD})
  FORCE=1           run anyway on a busy machine; mark the numbers as noisy
  SC_SETTLE_SECONDS longest wait for compaction to settle (${SETTLE_SECONDS})
  SC_SERVER_ARGS    extra server flags, e.g. "--cpu_threads 16" (none)

Results: ${RESULTS_DIR}/schema-change-<stamp>.tsv
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi

read -ra ENGINES <<<"${SC_ENGINES:-old:${OLD_BIN} new:${NEW_BIN}}"
for engine in "${ENGINES[@]}"; do
	[[ -x "${engine#*:}" ]] || {
		echo "missing binary: ${engine#*:}" >&2
		exit 1
	}
done
for tool in psql ss; do
	command -v "${tool}" >/dev/null || {
		echo "missing tool: ${tool}" >&2
		exit 1
	}
done
for case_name in ${CASES}; do
	[[ " ${ALL_CASES} " == *" ${case_name} "* ]] || {
		echo "unknown case: ${case_name}" >&2
		exit 1
	}
done

mkdir -p "${RESULTS_DIR}"
CONN="postgres://postgres@127.0.0.1:${PORT}/postgres"
SERVER_PID=""
DATA_DIR=""
TEMPLATES=()
TEMPLATE_DIR=""

load1() { cut -d' ' -f1 /proc/loadavg; }

check_load() {
	local load
	load="$(load1)"
	if awk -v l="${load}" -v m="${MAX_LOAD}" 'BEGIN { exit !(l > m) }'; then
		if [[ "${FORCE}" != 1 ]]; then
			echo "load average ${load} is above ${MAX_LOAD}: the machine is busy." >&2
			top -b -n 1 -o %CPU | head -15 >&2
			echo "run on a quiet machine, or set FORCE=1 to record noisy numbers" >&2
			exit 1
		fi
	fi
}

stop_server() {
	if [[ -n "${SERVER_PID}" ]]; then
		kill "${SERVER_PID}" >/dev/null 2>&1 || true
		wait "${SERVER_PID}" 2>/dev/null || true
	fi
	SERVER_PID=""
}

cleanup() {
	stop_server
	[[ -n "${DATA_DIR}" ]] && rm -rf "${DATA_DIR}"
	for dir in "${TEMPLATES[@]}"; do
		rm -rf "${dir}"
	done
}
trap cleanup EXIT

start_server() {
	local bin="$1" dir="$2"
	if [[ -n "$(ss -ltnH "sport = :${PORT}")" ]]; then
		echo "port ${PORT} is already in use" >&2
		exit 1
	fi
	"${bin}" "${dir}" --listen "postgres://127.0.0.1:${PORT}" "${SERVER_ARGS[@]}" \
		>>"${dir}.log" 2>&1 &
	SERVER_PID=$!
	for _ in $(seq 1 240); do
		kill -0 "${SERVER_PID}" 2>/dev/null || {
			echo "server exited, see ${dir}.log" >&2
			tail -20 "${dir}.log" >&2
			exit 1
		}
		psql "${CONN}" -qtAX -c 'SELECT 1' >/dev/null 2>&1 && return 0
		sleep 0.5
	done
	echo "server did not start, see ${dir}.log" >&2
	exit 1
}

sql() { psql "${CONN}" -qtAX -v ON_ERROR_STOP=1 -c "$1"; }

now_ms() { date +%s%3N; }

written_bytes() { awk '/^write_bytes/ { print $2 }' "/proc/${SERVER_PID}/io"; }

wal_bytes() {
	sql "SELECT value FROM sdb_metrics WHERE metric = 'catalog_wal_appended_bytes'"
}

table_metric() {
	sql "SELECT value FROM sdb_metrics
	     WHERE relation_id = (SELECT oid FROM pg_class WHERE relname = 't')
	       AND metric = '$1'"
}

store_bytes() { { du -sb "${DATA_DIR}/engine_v1/store" 2>/dev/null || true; } | cut -f1; }

settle() {
	local last="" same=0 now
	for _ in $(seq 1 $((SETTLE_SECONDS * 4))); do
		now="$(table_metric num_segments)/$(table_metric num_buffered_docs)/$(table_metric num_files)"
		if [[ "${now}" == "${last}" ]]; then
			same=$((same + 1))
			[[ ${same} -ge 8 ]] && return 0
		else
			same=0
			last="${now}"
		fi
		sleep 0.25
	done
	echo "compaction did not settle within ${SETTLE_SECONDS}s" >&2
}

build_template() {
	local bin="$1" dir
	dir="$(mktemp -d "${DATA_ROOT}/${USER}-schema-change-XXXXXX")"
	TEMPLATES+=("${dir}")
	DATA_DIR="${dir}"
	start_server "${bin}" "${dir}"
	sql "CREATE TEXT SEARCH DICTIONARY sc_en AS split_text(case := 'lower')
	     WITH (frequency, position, norm)"
	sql "CREATE TABLE t (id BIGINT, n INT, n2 BIGINT, body TEXT, tag TEXT, payload TEXT)"
	sql "INSERT INTO t
	     SELECT i, (i % 100000)::INT, i,
	            'w' || (i * 7919 % 5000) || ' w' || (i * 104729 % 5000) ||
	            ' w' || (i * 1299709 % 5000) || ' w' || (i * 15485863 % 5000) ||
	            ' w' || (i % 5000) || ' w' || (i * 31 % 5000),
	            'tag' || (i % 50), md5(i::VARCHAR)
	     FROM generate_series(1::BIGINT, ${ROWS}::BIGINT) g(i)"
	sql "CHECKPOINT"
	settle
	stop_server
	DATA_DIR=""
	TEMPLATE_DIR="${dir}"
}

case_setup() {
	case "$1" in
	create_index_second)
		sql "CREATE INDEX t_tag ON t USING inverted (tag)"
		settle
		;;
	esac
}

case_statement() {
	case "$1" in
	create_index_text) echo "CREATE INDEX t_body ON t USING inverted (body sc_en)" ;;
	create_index_verbatim) echo "CREATE INDEX t_tag ON t USING inverted (tag)" ;;
	create_index_second) echo "CREATE INDEX t_body ON t USING inverted (body sc_en)" ;;
	add_column_null) echo "ALTER TABLE t ADD COLUMN c INT" ;;
	add_column_const) echo "ALTER TABLE t ADD COLUMN c INT DEFAULT 42" ;;
	add_column_volatile) echo "ALTER TABLE t ADD COLUMN c DOUBLE DEFAULT random()" ;;
	alter_type) echo "ALTER TABLE t ALTER COLUMN n TYPE BIGINT" ;;
	add_unique) echo "ALTER TABLE t ADD UNIQUE (n2)" ;;
	add_pk) echo "ALTER TABLE t ADD PRIMARY KEY (id)" ;;
	esac
}

case_check() {
	case "$1" in
	create_index_text | create_index_second) echo "SELECT count(*) FROM t WHERE body @@ 'w17'" ;;
	create_index_verbatim) echo "SELECT count(*) FROM t WHERE tag = 'tag7'" ;;
	add_column_null | add_column_volatile) echo "SELECT count(c) FROM t" ;;
	add_column_const) echo "SELECT sum(c) FROM t" ;;
	alter_type) echo "SELECT sum(n) FROM t" ;;
	add_unique) echo "SELECT count(*) FROM t WHERE n2 = 12345" ;;
	add_pk) echo "SELECT count(*) FROM t WHERE id = 777" ;;
	esac
}

run_case() {
	local rep="$1" engine="$2" bin="$3" template="$4" case_name="$5"
	local load t0 t1 w0 w1 wal0 wal1 bytes files segments check="" statement
	check_load
	DATA_DIR="$(mktemp -d "${DATA_ROOT}/${USER}-schema-change-XXXXXX")"
	cp -a "${template}/." "${DATA_DIR}/"
	start_server "${bin}" "${DATA_DIR}"
	case_setup "${case_name}"
	statement="$(case_statement "${case_name}")"
	load="$(load1)"
	w0="$(written_bytes)"
	wal0="$(wal_bytes)"
	t0="$(now_ms)"
	if ! sql "${statement}" >/dev/null 2>"${DATA_DIR}.err"; then
		echo "${case_name} failed on ${engine}: $(cat "${DATA_DIR}.err")" >&2
		check="error"
	fi
	t1="$(now_ms)"
	w1="$(written_bytes)"
	wal1="$(wal_bytes)"
	bytes="$(store_bytes)"
	files="$(table_metric num_files)"
	segments="$(table_metric num_segments)"
	[[ "${check:-}" == error ]] || check="$(sql "$(case_check "${case_name}")")"
	stop_server
	rm -rf "${DATA_DIR}" "${DATA_DIR}.log" "${DATA_DIR}.err"
	DATA_DIR=""
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"${rep}" "${case_name}" "${engine}" "${load}" "$((t1 - t0))" \
		"$((w1 - w0))" "$((wal1 - wal0))" "${bytes}" "${files}" "${segments}" "${check}" \
		>>"${OUT}"
	printf '%-3s %-22s %-6s %8s %9.1f %6s %5s  %s\n' \
		"${rep}" "${case_name}" "${engine}" "$((t1 - t0))" \
		"$(awk -v b="$((w1 - w0))" 'BEGIN { print b / 1048576 }')" \
		"${files}" "${segments}" "${check}"
}

summarize() {
	gawk -F'\t' -v first="${ENGINES[0]%%:*}" '
		/^#/ || $1 == "rep" { next }
		{
			key = $2 SUBSEP $3
			if (!(key in n)) { order[++count] = key }
			i = ++n[key]
			ms[key, i] = $5; mb[key, i] = $6 / 1048576
		}
		function median(values, key, k,    a, j) {
			for (j = 1; j <= k; ++j) a[j] = values[key, j]
			asort(a)
			return k % 2 ? a[(k + 1) / 2] : (a[k / 2] + a[k / 2 + 1]) / 2
		}
		function delta(value, base) {
			return base > 0 ? sprintf("%+.0f%%", 100 * (value - base) / base) : "-"
		}
		END {
			printf "\nMedians; %% against %s (- is better)\n", first
			printf "%-22s %-6s %8s %6s %9s %6s\n", "case", "engine", "ms", "", "MB", ""
			for (c = 1; c <= count; ++c) {
				key = order[c]; split(key, part, SUBSEP)
				base = part[1] SUBSEP first
				m = median(ms, key, n[key]); bm = median(ms, base, n[base])
				b = median(mb, key, n[key]); bb = median(mb, base, n[base])
				printf "%-22s %-6s %8.0f %6s %9.1f %6s\n", part[1], part[2], m, delta(m, bm), b, delta(b, bb)
			}
		}' "${OUT}"
}

check_load
{
	for engine in "${ENGINES[@]}"; do
		echo "# ${engine%%:*}=${engine#*:}"
	done
	echo "# rows=${ROWS} reps=${REPS} force=${FORCE} host=$(hostname) nproc=$(nproc) server_args=${SC_SERVER_ARGS:-}"
} | tee "${OUT}"
printf 'rep\tcase\tengine\tload1\tms\twrite_bytes\twal_bytes\tstore_bytes\tfiles\tsegments\tcheck\n' >>"${OUT}"
printf '%-3s %-22s %-6s %8s %9s %6s %5s  %s\n' rep case engine ms MB files segs check
echo "  ms: the statement's response time; MB: bytes written by it; files, segs: right after it"

declare -A TEMPLATE_OF
for rep in $(seq 1 "${REPS}"); do
	for engine in "${ENGINES[@]}"; do
		build_template "${engine#*:}"
		TEMPLATE_OF["${engine%%:*}"]="${TEMPLATE_DIR}"
	done
	for case_name in ${CASES}; do
		for engine in "${ENGINES[@]}"; do
			run_case "${rep}" "${engine%%:*}" "${engine#*:}" \
				"${TEMPLATE_OF["${engine%%:*}"]}" "${case_name}"
		done
	done
	for template in "${TEMPLATES[@]}"; do
		rm -rf "${template}" "${template}.log"
	done
	TEMPLATES=()
done
summarize
echo "results in ${OUT}"
