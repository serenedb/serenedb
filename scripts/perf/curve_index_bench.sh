#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
SERENED_BIN="${CURVE_BENCH_BIN:-${ROOT}/build_perf/bin/serened}"
ROWS="${CURVE_BENCH_ROWS:-1000000}"
BOXES="${CURVE_BENCH_BOXES:-50}"
PASSES="${CURVE_BENCH_PASSES:-3}"
SEED="${CURVE_BENCH_SEED:-1099}"
THREADS="${CURVE_BENCH_THREADS:-0 1}"
TABLES="${CURVE_BENCH_TABLES:-ind cor}"
INDEXES="${CURVE_BENCH_INDEXES:-granular curve curve32 norm32}"
PORT="${CURVE_BENCH_PORT:-6471}"
SEGMENT_MEMORY_MAX="${CURVE_BENCH_SEGMENT_MEMORY_MAX:-}"
REFRESHES="${CURVE_BENCH_REFRESHES:-1}"
COMPACT="${CURVE_BENCH_COMPACT:-full}"
if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN}; set CURVE_BENCH_BIN" >&2
	exit 1
fi
for value in "${ROWS}" "${BOXES}" "${PASSES}" "${SEED}" "${REFRESHES}"; do
	if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
		echo "CURVE_BENCH_ROWS, _BOXES, _PASSES, _SEED and _REFRESHES must be positive integers" >&2
		exit 1
	fi
done
if [[ ! "${COMPACT}" =~ ^(full|background|none)$ ]]; then
	echo "CURVE_BENCH_COMPACT must be full, background or none" >&2
	exit 1
fi
INDEX_OPTIONS=""
if [[ -n "${SEGMENT_MEMORY_MAX}" ]]; then
	if [[ ! "${SEGMENT_MEMORY_MAX}" =~ ^[1-9][0-9]*$ ]]; then
		echo "CURVE_BENCH_SEGMENT_MEMORY_MAX must be a positive byte count" >&2
		exit 1
	fi
	INDEX_OPTIONS="segment_memory_max = ${SEGMENT_MEMORY_MAX}"
fi
if [[ "${COMPACT}" == none ]]; then
	INDEX_OPTIONS="${INDEX_OPTIONS:+${INDEX_OPTIONS}, }compaction_interval = 0"
fi
INDEX_OPTIONS="${INDEX_OPTIONS:+ WITH (${INDEX_OPTIONS})}"

declare -A CALLS=(
	[curve]="curve(ROW(x, y, z))"
	[hilbert]="curve(ROW(x, y, z), kind := 'hilbert')"
	[curve32]="curve(ROW(x, y, z), max_level := 32)"
	[norm32]="curve(ROW(x, y, z), max_level := 32, lower := ROW(-1, -1, -1), upper := ROW(1001, 1001, 1001))"
)

DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/curve-bench-XXXXXX")"
LOG="${DATA_DIR}.log"

"${SERENED_BIN}" "${DATA_DIR}" --listen "postgres://127.0.0.1:${PORT}" >"${LOG}" 2>&1 &
SERENED_PID=$!
trap 'kill -9 ${SERENED_PID} >/dev/null 2>&1 || true; rm -rf "${DATA_DIR}"' EXIT

PSQL=(psql -h 127.0.0.1 -p "${PORT}" -U postgres -d postgres -X -q -v ON_ERROR_STOP=1)
READY=false
for _ in $(seq 1 120); do
	if ! kill -0 "${SERENED_PID}" >/dev/null 2>&1; then
		echo "server failed to start; see ${LOG}" >&2
		exit 1
	fi
	if grep -q 'ready for business' "${LOG}" &&
		"${PSQL[@]}" -c 'SELECT 1' >/dev/null 2>&1; then
		READY=true
		break
	fi
	sleep 0.5
done
if [[ "${READY}" != true ]]; then
	echo "server did not become ready; see ${LOG}" >&2
	exit 1
fi

sql() {
	"${PSQL[@]}" -At -c "$1"
}

seconds() {
	local start end
	start=$(date +%s%N)
	sql "$1" >/dev/null
	end=$(date +%s%N)
	awk -v ns="$((end - start))" 'BEGIN { printf "%.2f", ns / 1e9 }'
}

coprime_modulus() {
	local modulus a b remainder
	modulus=$((ROWS + $1))
	while true; do
		a=${modulus}
		b=$2
		while ((b != 0)); do
			remainder=$((a % b))
			a=${b}
			b=${remainder}
		done
		if ((a == 1)); then
			echo "${modulus}"
			return
		fi
		modulus=$((modulus + 1))
	done
}

unsettled() {
	sql "SELECT count(*) FROM sdb_metrics m JOIN pg_class c ON c.oid = m.relation_id
		WHERE c.relname LIKE '$1_%' AND (m.metric = 'num_segments' AND m.value > 1
		OR m.metric IN ('compaction_active', 'compaction_pending') AND m.value > 0)"
}

compact() {
	sql "VACUUM (COMPACT_TABLE) $1" >/dev/null
	while [[ "$(unsettled "$1")" != 0 ]]; do
		sleep 1
		sql "VACUUM (COMPACT_TABLE) $1" >/dev/null
	done
}

settle() {
	local state previous="" quiet=0
	while ((quiet < 5)); do
		sleep 2
		state=$(sql "SELECT coalesce(sum(CASE WHEN m.metric = 'num_segments' THEN m.value END), 0),
			coalesce(sum(CASE WHEN m.metric <> 'num_segments' THEN m.value END), 0)
			FROM sdb_metrics m JOIN pg_class c ON c.oid = m.relation_id
			WHERE c.relname LIKE '$1_%' AND m.metric IN ('num_segments', 'compaction_active', 'compaction_pending')")
		if [[ "${state}" == "${previous}" && "${state#*|}" == 0 ]]; then
			quiet=$((quiet + 1))
		else
			quiet=0
		fi
		previous="${state}"
	done
}

boxes() {
	awk -v table="$1" -v half="$2" -v axes="$3" -v seed="$4" -v count="${BOXES}" 'BEGIN {
		srand(seed + 10 * half + axes)
		for (i = 0; i < count; i++) {
			x = 100 + rand() * 800
			y = table == "cor" ? x : 100 + rand() * 800
			z = table == "cor" ? x : 100 + rand() * 800
			if (axes == 1) {
				printf "%.3f NULL NULL %.3f NULL NULL\n", x - half, x + half
			} else {
				printf "%.3f %.3f %.3f %.3f %.3f %.3f\n", x - half, y - half, z - half, x + half, y + half, z + half
			}
		}
	}'
}

statements() {
	local kind where
	while read -r x0 y0 z0 x1 y1 z1; do
		where="x BETWEEN ${x0} AND ${x1}"
		if [[ "${y0}" != NULL ]]; then
			where+=" AND y BETWEEN ${y0} AND ${y1} AND z BETWEEN ${z0} AND ${z1}"
		fi
		printf 'table\tSELECT %s FROM %s WHERE %s;\n' "$2" "$1" "${where}"
		for kind in ${INDEXES}; do
			if [[ "${kind}" == granular ]]; then
				printf 'granular\tSELECT %s FROM %s_granular WHERE %s;\n' "$2" "$1" "${where}"
			else
				printf '%s\tSELECT %s FROM %s_%s WHERE %s @@ ts_between(ROW(%s, %s, %s), ROW(%s, %s, %s), true, true);\n' \
					"${kind}" "$2" "$1" "${kind}" "${CALLS[${kind}]}" "${x0}" "${y0}" "${z0}" "${x1}" "${y1}" "${z1}"
			fi
		done
	done <<<"$3"
}

timed() {
	local shuffled
	shuffled=$(shuf --random-source=<(yes "$1") <<<"$3")
	paste <(cut -f1 <<<"${shuffled}") <({
		if [[ "$2" != 0 ]]; then
			echo "SET threads = $2;"
		fi
		echo '\timing on'
		cut -f2 <<<"${shuffled}"
	} | "${PSQL[@]}" -At | awk '/^Time: /{print $2}')
}

summary() {
	sort -k1,1 -k2,2g | awk '
		function flush() {
			if (n) {
				printf " %s %.3f/%.3f", kind, sum / n, v[int((n + 1) / 2)]
			}
		}
		$1 != kind {
			flush()
			kind = $1
			n = 0
			sum = 0
		}
		{
			v[++n] = $2
			sum += $2
		}
		END {
			flush()
		}
	'
}

verify() {
	local list expected kind
	list=$(statements "$1" 'count(*), max(id)' "$3")
	expected=$(awk -F'\t' '$1 == "table" {print $2}' <<<"${list}" | "${PSQL[@]}" -At)
	for kind in ${INDEXES}; do
		if [[ "$(awk -F'\t' -v kind="${kind}" '$1 == kind {print $2}' <<<"${list}" | "${PSQL[@]}" -At)" != "${expected}" ]]; then
			echo "result mismatch: $1 ${kind} $2" >&2
			exit 1
		fi
	done
	awk -F'|' '{print $1}' <<<"${expected}"
}

measure() {
	local t=$1 half=$2 axes=$3 threads=$4 label=$5 pass list times all="" rows=""
	timed "${SEED}" "${threads}" "$(statements "${t}" 'max(id)' "$(boxes "${t}" "${half}" "${axes}" "${SEED}")")" >/dev/null
	for pass in $(seq 1 "${PASSES}"); do
		list=$(boxes "${t}" "${half}" "${axes}" "$((SEED + pass))")
		times=$(timed "$((SEED + pass))" "${threads}" "$(statements "${t}" 'max(id)' "${list}")")
		all+="${times}"$'\n'
		echo "pass ${pass} ${t} ${label} threads ${threads}:$(summary <<<"${times}")"
		rows+="$(verify "${t}" "${label}" "${list}")"$'\n'
	done
	echo "query ${t} ${label} threads ${threads}: mean rows $(awk 'NF {sum += $1; n++} END {printf "%.0f", sum / n}' <<<"${rows}"), mean/median ms$(sed '/^$/d' <<<"${all}" | summary)"
}

X_MOD=$(coprime_modulus 3 48271)
Y_MOD=$(coprime_modulus 33 69621)
Z_MOD=$(coprime_modulus 37 40699)
SCALE=$((ROWS >= 1000 ? ROWS / 1000 : 1))

fill() {
	sql "INSERT INTO ind SELECT i, ((i*48271)%${X_MOD})::DOUBLE/${SCALE},
		((i*69621+123)%${Y_MOD})::DOUBLE/${SCALE}, ((i*40699+321)%${Z_MOD})::DOUBLE/${SCALE}
		FROM generate_series($1::BIGINT, $2::BIGINT) g(i)"
	if [[ " ${TABLES} " == *" cor "* ]]; then
		sql "INSERT INTO cor SELECT id, x, x+(id%7-3)*0.1, x+(id%11-5)*0.1 FROM ind WHERE id BETWEEN $1 AND $2"
	fi
}

index_of() {
	local keys
	if [[ "$2" == granular ]]; then
		keys="id, x, y, z)"
	else
		keys="id, (${CALLS[$2]}))"
	fi
	echo "CREATE INDEX $1_$2 ON $1 USING inverted(${keys}${INDEX_OPTIONS}"
}

metric() {
	sql "SELECT round(m.value / $3)::BIGINT FROM sdb_metrics m JOIN pg_class c ON c.oid = m.relation_id
		WHERE c.relname = '$1' AND m.metric = '$2'"
}

sql "CREATE TABLE ind(id BIGINT PRIMARY KEY, x DOUBLE, y DOUBLE, z DOUBLE)"
if [[ " ${TABLES} " == *" cor "* ]]; then
	sql "CREATE TABLE cor(id BIGINT PRIMARY KEY, x DOUBLE, y DOUBLE, z DOUBLE)"
fi

if ((REFRESHES == 1)); then
	fill 1 "${ROWS}"
	for t in ${TABLES}; do
		line="build ${t}:"
		for kind in ${INDEXES}; do
			line+=" ${kind} $(seconds "$(index_of "${t}" "${kind}"); VACUUM (REFRESH_TABLE) ${t}") s"
		done
		echo "${line}"
	done
else
	for t in ${TABLES}; do
		for kind in ${INDEXES}; do
			sql "$(index_of "${t}" "${kind}")"
		done
	done
	for ((batch = 0; batch < REFRESHES; batch++)); do
		fill "$((ROWS * batch / REFRESHES + 1))" "$((ROWS * (batch + 1) / REFRESHES))"
		for t in ${TABLES}; do
			sql "VACUUM (REFRESH_TABLE) ${t}" >/dev/null
		done
	done
fi
start=$(date +%s)
for t in ${TABLES}; do
	case "${COMPACT}" in
	full) compact "${t}" ;;
	background) settle "${t}" ;;
	esac
done
echo "compact: $(($(date +%s) - start)) s"
for t in ${TABLES}; do
	for kind in ${INDEXES}; do
		echo "size ${t}_${kind}: $(metric "${t}_${kind}" index_size 1048576.0) MB, $(metric "${t}_${kind}" num_segments 1) segments"
	done
done

for threads in ${THREADS}; do
	for t in ${TABLES}; do
		for half in 0.5 5 25 50 100; do
			measure "${t}" "${half}" 3 "${threads}" "box ±${half}"
		done
		measure "${t}" 5 1 "${threads}" "one axis"
	done
done
