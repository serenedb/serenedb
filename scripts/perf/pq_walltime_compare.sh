#!/usr/bin/env bash
# Wall-time CREATE INDEX over parquet view, per binary, per case, N iters.
# No perf-record (clean wall numbers).
set -u
N="${N:-5}"
ROWS="${ROWS:-1000000}"
CASES_DEFAULT="uuid url"
read -r -a CASES <<<"${CASES_STR:-$CASES_DEFAULT}"
export PGPASSWORD=postgres

bench_one() {
	local bin="$1" port="$2" label="$3" case="$4"
	local data="/tmp/pqw-${USER}-${label}-${case}"
	local parquet="/tmp/pqw-${USER}-${case}-${ROWS}.parquet"

	local pid
	pid=$(lsof -t -i:"$port" 2>/dev/null || true)
	[ -n "$pid" ] && kill -9 $pid 2>/dev/null || true
	rm -rf "$data"
	mkdir -p "$data"

	"$bin" "$data" --server_endpoints="pgsql+tcp://0.0.0.0:${port}" \
		>"/tmp/pqw-${USER}-${label}-${case}.log" 2>&1 &
	local sp=$!
	for _ in $(seq 1 60); do
		psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -tAc 'SELECT 1' \
			>/dev/null 2>&1 && break
		sleep 0.3
	done

	local PSQL="psql -h 127.0.0.1 -p $port -U postgres -d postgres -v ON_ERROR_STOP=1"
	local MAN
	MAN="$(dirname "$0")/iresearch_sweep/cases.manifest"
	IFS=$'\t' read -r _name _idx_clause _query _online _med _large \
		<<<"$(awk -F'\t' -v c="$case" '$1==c {print; exit}' "$MAN")"

	# Setup data into sweep_src, COPY to parquet, expose as sweep_t view
	local setup_sql
	setup_sql=$(sed "s/@N@/${ROWS}/g; s/sweep_t/sweep_src/g; s/sweep_idx/sweep_src_idx/g" \
		"$(dirname "$0")/iresearch_sweep/cases/${case}.sql")
	$PSQL <<<"$setup_sql" >/dev/null 2>&1
	rm -f "$parquet"
	$PSQL -c "COPY sweep_src TO '$parquet' (FORMAT PARQUET);" >/dev/null
	$PSQL -c "DROP VIEW IF EXISTS sweep_t;" >/dev/null
	$PSQL -c "CREATE VIEW sweep_t AS SELECT * FROM read_parquet('$parquet');" >/dev/null

	local create_idx="CREATE INDEX sweep_idx ON sweep_t USING inverted(${_idx_clause});"
	local drop_idx="DROP INDEX IF EXISTS sweep_idx;"

	echo "[$label/$case]"
	for i in $(seq 1 "$N"); do
		$PSQL -c "$drop_idx" >/dev/null
		local t0 t1
		t0=$(date +%s.%N)
		$PSQL -c "$create_idx" >/dev/null
		t1=$(date +%s.%N)
		awk -v t0="$t0" -v t1="$t1" -v i="$i" \
			'BEGIN { printf "  run %d: wall=%.3fs\n", i, t1 - t0 }'
	done

	kill -9 $sp 2>/dev/null || true
	rm -rf "$data"
}

declare -A BINS=(
	[push_inline]="/tmp/serened_push_inline:6280"
	[simplified]="/tmp/serened_simplified:6281"
)

for case in "${CASES[@]}"; do
	for label in push_inline simplified; do
		spec="${BINS[$label]}"
		bin="${spec%:*}"
		port="${spec#*:}"
		bench_one "$bin" "$port" "$label" "$case"
	done
done
