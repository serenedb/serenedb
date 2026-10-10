#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
SERENED_BIN="${CURVE_BENCH_BIN:-${ROOT}/build_bench/bin/serened}"
ROWS="${CURVE_BENCH_ROWS:-1000000}"
REPS="${CURVE_BENCH_REPS:-7}"
PORT="${CURVE_BENCH_PORT:-6471}"
SEGMENT_MEMORY_MAX="${CURVE_BENCH_SEGMENT_MEMORY_MAX:-}"
DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/curve-bench-XXXXXX")"
LOG="${DATA_DIR}.log"

if [[ ! -x "${SERENED_BIN}" ]]; then
	echo "missing ${SERENED_BIN}; set CURVE_BENCH_BIN" >&2
	exit 1
fi
if [[ ! "${ROWS}" =~ ^[1-9][0-9]*$ ]]; then
	echo "CURVE_BENCH_ROWS must be a positive integer" >&2
	exit 1
fi
INDEX_OPTIONS=""
if [[ -n "${SEGMENT_MEMORY_MAX}" ]]; then
	if [[ ! "${SEGMENT_MEMORY_MAX}" =~ ^[1-9][0-9]*$ ]]; then
		echo "CURVE_BENCH_SEGMENT_MEMORY_MAX must be a positive byte count" >&2
		exit 1
	fi
	INDEX_OPTIONS=" WITH (segment_memory_max = ${SEGMENT_MEMORY_MAX})"
fi

"${SERENED_BIN}" "${DATA_DIR}" --listen "postgres://127.0.0.1:${PORT}" >"${LOG}" 2>&1 &
SERENED_PID=$!
trap 'kill -9 ${SERENED_PID} >/dev/null 2>&1 || true; rm -rf "${DATA_DIR}"' EXIT

PSQL=(psql -h 127.0.0.1 -p "${PORT}" -U postgres -d postgres -X -q -v ON_ERROR_STOP=1)
for _ in $(seq 1 120); do
	if "${PSQL[@]}" -c 'SELECT 1' >/dev/null 2>&1; then
		break
	fi
	sleep 0.5
done

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

median_ms() {
	sql "$1" >/dev/null
	for _ in $(seq 1 "${REPS}"); do
		"${PSQL[@]}" -At -c '\timing on' -c "$1" | awk '/^Time: /{print $2}'
	done | sort -g | awk '{v[NR] = $1} END {print v[int((NR + 1) / 2)]}'
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

check() {
	local expected
	expected=$(sql "SELECT count(*), max(id) $1")
	for query in "${@:2}"; do
		if [[ "$(sql "SELECT count(*), max(id) ${query}")" != "${expected}" ]]; then
			echo "result mismatch: ${query}" >&2
			exit 1
		fi
	done
	echo "${expected%%|*}"
}

X_MOD=$(coprime_modulus 3 48271)
Y_MOD=$(coprime_modulus 33 69621)
Z_MOD=$(coprime_modulus 37 40699)
SCALE=$((ROWS >= 1000 ? ROWS / 1000 : 1))

sql "CREATE TABLE ind(id BIGINT PRIMARY KEY, x DOUBLE, y DOUBLE, z DOUBLE)"
sql "INSERT INTO ind SELECT i, ((i*48271)%${X_MOD})::DOUBLE/${SCALE},
	((i*69621+123)%${Y_MOD})::DOUBLE/${SCALE}, ((i*40699+321)%${Z_MOD})::DOUBLE/${SCALE}
	FROM generate_series(1, ${ROWS}) g(i)"
sql "CREATE TABLE cor(id BIGINT PRIMARY KEY, x DOUBLE, y DOUBLE, z DOUBLE)"
sql "INSERT INTO cor SELECT id, x, x+(id%7-3)*0.1, x+(id%11-5)*0.1 FROM ind"

for t in ind cor; do
	g=$(seconds "CREATE INDEX ${t}_granular ON ${t} USING inverted(id, x, y, z)${INDEX_OPTIONS}; VACUUM (REFRESH_TABLE) ${t}")
	c=$(seconds "CREATE INDEX ${t}_curve ON ${t} USING inverted(id, (ROW(x, y, z)) curve()) INCLUDE (x, y, z)${INDEX_OPTIONS}; VACUUM (REFRESH_TABLE) ${t}")
	d=$(seconds "CREATE INDEX ${t}_curve32 ON ${t} USING inverted(id, (ROW(x, y, z)) curve(max_level=32)) INCLUDE (x, y, z)${INDEX_OPTIONS}; VACUUM (REFRESH_TABLE) ${t}")
	echo "build ${t}: granular ${g} s, curve ${c} s, curve max_level=32 ${d} s"
done

for t in ind cor; do
	for half in 0.5 5 25 50 100; do
		lo=$(awk -v h="${half}" 'BEGIN {print 500 - h}')
		hi=$(awk -v h="${half}" 'BEGIN {print 500 + h}')
		between="x BETWEEN ${lo} AND ${hi} AND y BETWEEN ${lo} AND ${hi} AND z BETWEEN ${lo} AND ${hi}"
		box="sdb_box_contains(ROW(x, y, z), ROW(${lo}::DOUBLE, ${lo}::DOUBLE, ${lo}::DOUBLE), ROW(${hi}::DOUBLE, ${hi}::DOUBLE, ${hi}::DOUBLE))"
		s="FROM ${t} WHERE ${between}"
		g="FROM ${t}_granular WHERE ${between}"
		c="FROM ${t}_curve WHERE ${box}"
		d="FROM ${t}_curve32 WHERE ${box}"
		rows=$(check "${s}" "${g}" "${c}" "${d}")
		echo "query ${t} box ±${half}: rows ${rows}, max(id) ms table $(median_ms "SELECT max(id) ${s}") granular $(median_ms "SELECT max(id) ${g}") curve $(median_ms "SELECT max(id) ${c}") curve max_level=32 $(median_ms "SELECT max(id) ${d}")"
	done
	box="sdb_box_contains(ROW(x, y, z), ROW(495::DOUBLE, NULL, NULL), ROW(505::DOUBLE, NULL, NULL))"
	s="FROM ${t} WHERE x BETWEEN 495 AND 505"
	g="FROM ${t}_granular WHERE x BETWEEN 495 AND 505"
	c="FROM ${t}_curve WHERE ${box}"
	d="FROM ${t}_curve32 WHERE ${box}"
	rows=$(check "${s}" "${g}" "${c}" "${d}")
	echo "query ${t} one axis: rows ${rows}, max(id) ms table $(median_ms "SELECT max(id) ${s}") granular $(median_ms "SELECT max(id) ${g}") curve $(median_ms "SELECT max(id) ${c}") curve max_level=32 $(median_ms "SELECT max(id) ${d}")"
done

for t in ind cor; do
	sql "VACUUM (COMPACT_TABLE) ${t}" >/dev/null
done
for index in ind_granular ind_curve ind_curve32 cor_granular cor_curve cor_curve32; do
	size=$(sql "SELECT round(m.value / 1048576.0)::BIGINT FROM sdb_metrics m JOIN pg_class c ON c.oid = m.relation_id
		WHERE c.relname = '${index}' AND m.metric = 'index_size'")
	echo "size ${index}: ${size} MB"
done
