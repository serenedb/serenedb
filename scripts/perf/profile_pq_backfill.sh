#!/usr/bin/env bash
# profile_pq_backfill.sh -- perf-record over serened during CREATE INDEX on a
# parquet view, for every term-dict case. Bypasses RocksDB so the pareto is
# iresearch-dominant.
#
# Usage: profile_pq_backfill.sh <binary> <label> <out_dir> <port> [CASES...]
set -euo pipefail
BIN="$1"
LABEL="$2"
OUT="$3"
PORT="${4:-6171}"
shift 4
CASES=("$@")
if [[ ${#CASES[@]} -eq 0 ]]; then
	CASES=(english_simple ngram uuid url integer bigint boolean)
fi
ROWS_DEFAULT="${ROWS:-1000000}"
mkdir -p "$OUT"
export PGPASSWORD=postgres

run_case() {
	local case="$1" rows="$2"
	local data="/tmp/pq-prof-${USER}-${LABEL}-${case}"
	local parquet="/tmp/pq-prof-${USER}-${case}-${rows}.parquet"
	local outprefix="${OUT}/${LABEL}_${case}_${rows}"

	for p in "$PORT"; do
		local pid
		pid=$(lsof -t -i:$p 2>/dev/null || true)
		[ -n "$pid" ] && kill -9 $pid || true
	done
	rm -rf "$data"
	mkdir -p "$data"

	"$BIN" "$data" --server_endpoints="pgsql+tcp://0.0.0.0:${PORT}" \
		>"${outprefix}.serened.log" 2>&1 &
	local sp=$!
	for _ in $(seq 1 60); do
		psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -tAc 'SELECT 1' >/dev/null 2>&1 && break
		sleep 0.3
	done

	local PSQL="psql -h 127.0.0.1 -p $PORT -U postgres -d postgres -v ON_ERROR_STOP=1"

	# Look up case
	local MAN="$(dirname "$0")/iresearch_sweep/cases.manifest"
	IFS=$'\t' read -r _name _idx_clause _query _online _med _large <<<"$(awk -F'\t' -v c="$case" '$1==c {print; exit}' "$MAN")"

	# Setup: case SQL (renamed to sweep_src)
	local setup_sql
	setup_sql=$(sed "s/@N@/${rows}/g; s/sweep_t/sweep_src/g; s/sweep_idx/sweep_src_idx/g" \
		"$(dirname "$0")/iresearch_sweep/cases/${case}.sql")
	$PSQL <<<"$setup_sql" >"${outprefix}.setup.log" 2>&1

	# COPY to parquet (idempotent: rm + COPY)
	rm -f "$parquet"
	$PSQL -c "COPY sweep_src TO '$parquet' (FORMAT PARQUET);" >/dev/null

	# View backed by parquet
	$PSQL -c "DROP VIEW IF EXISTS sweep_t;" >/dev/null
	$PSQL -c "CREATE VIEW sweep_t AS SELECT * FROM read_parquet('$parquet');" >/dev/null

	# PERF RECORD over CREATE INDEX
	local create_idx="CREATE INDEX sweep_idx ON sweep_t USING inverted(${_idx_clause});"
	local t0 t1
	t0=$(date +%s.%N)
	perf record -F 999 -e cycles:P -p "$sp" -o "${outprefix}.perf.data" -- \
		bash -c "$PSQL -c \"$create_idx\" >/dev/null" 2>"${outprefix}.perf.stderr"
	t1=$(date +%s.%N)
	awk -v t0="$t0" -v t1="$t1" 'BEGIN { printf "%.4f\n", t1 - t0 }' >"${outprefix}.wall"

	perf report -i "${outprefix}.perf.data" --stdio --no-children --no-inline \
		-g none --percent-limit 0.3 >"${outprefix}.report" 2>&1

	kill -9 $sp 2>/dev/null || true
	rm -rf "$data"
	echo "$case rows=${rows} wall=$(cat ${outprefix}.wall)s"
}

for c in "${CASES[@]}"; do
	run_case "$c" "$ROWS_DEFAULT"
done
