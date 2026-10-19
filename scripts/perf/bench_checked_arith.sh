#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
	echo "usage: $0 <base serened> <new serened>" >&2
	exit 1
fi
BASE_BIN=$1
NEW_BIN=$2
ROWS=${ROWS:-100000000}
RUNS=${RUNS:-3}
ROUNDS=${ROUNDS:-6}
THREADS_LIST=${THREADS_LIST:-"1 8"}
CPUS=${CPUS:-16-23}
BASE_PORT=${BASE_PORT:-7895}
NEW_PORT=${NEW_PORT:-7896}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/checked_arith.XXXXXX")

QUERIES=(
	"SELECT sum(a*b+c) FROM t_bigint"
	"SELECT sum(a+b+c+d) FROM t_bigint"
	"SELECT sum(a*b) FROM t_int"
	"SELECT sum(a*b+c) FROM t_int"
	"SELECT sum(a*b) FROM t_int_nulls"
	"SELECT sum(a*b+c) FROM t_int_nulls"
	"SELECT sum(a*b) FROM t_smallint"
	"SELECT sum(x*y+z) FROM t_int"
)

psql_at() {
	psql -h 127.0.0.1 -p "$1" -U postgres -d postgres -At -q "${@:2}"
}

wait_ready() {
	for _ in $(seq 120); do
		psql_at "$1" -c "SELECT 1" >/dev/null 2>&1 && return 0
		sleep 1
	done
	echo "server on port $1 did not start" >&2
	exit 1
}

cleanup() {
	kill "${PIDS[@]}" 2>/dev/null || true
	wait 2>/dev/null || true
	rm -rf "$WORK"
}
PIDS=()
trap cleanup EXIT

"$NEW_BIN" "$WORK/setup" --listen="postgres://0.0.0.0:$NEW_PORT" >"$WORK/setup.log" 2>&1 &
PIDS+=($!)
wait_ready "$NEW_PORT"
psql_at "$NEW_PORT" <<EOF
CREATE TABLE t_bigint AS SELECT
  (CASE WHEN i = 0 THEN 5000000000000000000 ELSE i % 1000 END)::BIGINT AS a,
  (CASE WHEN i = 1000 THEN 5000000000000000000 ELSE (i * 7) % 1000 END)::BIGINT AS b,
  (CASE WHEN i = 2000 THEN 5000000000000000000 ELSE (i * 13) % 1000 END)::BIGINT AS c,
  (CASE WHEN i = 3000 THEN 5000000000000000000 ELSE (i * 31) % 1000 END)::BIGINT AS d
FROM range($ROWS) r(i);
CREATE TABLE t_int AS SELECT
  (CASE WHEN i = 0 THEN 2000000000 ELSE i % 1000 END)::INTEGER AS a,
  (CASE WHEN i = 1000 THEN 2000000000 ELSE (i * 7) % 1000 END)::INTEGER AS b,
  (CASE WHEN i = 2000 THEN 2000000000 ELSE (i * 13) % 1000 END)::INTEGER AS c,
  random() AS x, random() AS y, random() AS z
FROM range($ROWS) r(i);
CREATE TABLE t_int_nulls AS SELECT
  (CASE WHEN i = 0 THEN 2000000000 WHEN hash(i) % 10 = 0 THEN NULL ELSE i % 1000 END)::INTEGER AS a,
  (CASE WHEN i = 1000 THEN 2000000000 WHEN hash(i + 7) % 10 = 0 THEN NULL ELSE (i * 7) % 1000 END)::INTEGER AS b,
  (CASE WHEN i = 2000 THEN 2000000000 ELSE (i * 13) % 1000 END)::INTEGER AS c
FROM range($ROWS) r(i);
CREATE TABLE t_smallint AS SELECT
  (CASE WHEN i = 0 THEN 30000 ELSE i % 100 END)::SMALLINT AS a,
  (CASE WHEN i = 100 THEN 30000 ELSE (i * 7) % 100 END)::SMALLINT AS b,
  (CASE WHEN i = 200 THEN 30000 ELSE (i * 13) % 100 END)::SMALLINT AS c
FROM range($ROWS) r(i);
CHECKPOINT;
EOF
kill "${PIDS[@]}"
wait
PIDS=()
cp -a "$WORK/setup" "$WORK/base"
mv "$WORK/setup" "$WORK/new"

taskset -c "$CPUS" "$BASE_BIN" "$WORK/base" --listen="postgres://0.0.0.0:$BASE_PORT" >"$WORK/base.log" 2>&1 &
PIDS+=($!)
taskset -c "$CPUS" "$NEW_BIN" "$WORK/new" --listen="postgres://0.0.0.0:$NEW_PORT" >"$WORK/new.log" 2>&1 &
PIDS+=($!)
wait_ready "$BASE_PORT"
wait_ready "$NEW_PORT"

run_once() {
	psql_at "$1" <<EOF | sed -n 's/^Time: \([0-9.]*\) ms.*/\1/p'
SET threads = $2;
\timing on
$3;
EOF
}

for sql in "${QUERIES[@]}"; do
	base_res=$(psql_at "$BASE_PORT" -c "SET threads = 1; $sql")
	new_res=$(psql_at "$NEW_PORT" -c "SET threads = 1; $sql")
	if [[ "$base_res" != "$new_res" ]]; then
		echo "RESULT MISMATCH: $sql: $base_res vs $new_res" >&2
		exit 1
	fi
done

RAW="$WORK/raw.txt"
: >"$RAW"
for run in $(seq "$RUNS"); do
	for threads in $THREADS_LIST; do
		for q in "${!QUERIES[@]}"; do
			sql=${QUERIES[$q]}
			run_once "$BASE_PORT" "$threads" "$sql" >/dev/null
			run_once "$NEW_PORT" "$threads" "$sql" >/dev/null
			for round in $(seq "$ROUNDS"); do
				if ((round % 2)); then
					base_ms=$(run_once "$BASE_PORT" "$threads" "$sql")
					new_ms=$(run_once "$NEW_PORT" "$threads" "$sql")
				else
					new_ms=$(run_once "$NEW_PORT" "$threads" "$sql")
					base_ms=$(run_once "$BASE_PORT" "$threads" "$sql")
				fi
				echo "$threads $q $base_ms $new_ms" >>"$RAW"
			done
		done
	done
done

for threads in $THREADS_LIST; do
	echo "threads=$threads"
	printf "%-42s %10s %10s %8s %s\n" "query" "base ms" "new ms" "speedup" "pair min..max"
	for q in "${!QUERIES[@]}"; do
		awk -v t="$threads" -v q="$q" -v sql="${QUERIES[$q]}" '
			$1 == t && $2 == q { n++; b[n] = $3; w[n] = $4; r[n] = $3 / $4 }
			function median(a, n,   i, j, x) {
				for (i = 2; i <= n; i++) { x = a[i]; for (j = i - 1; j >= 1 && a[j] > x; j--) a[j + 1] = a[j]; a[j + 1] = x }
				return n % 2 ? a[(n + 1) / 2] : (a[n / 2] + a[n / 2 + 1]) / 2
			}
			END {
				mn = 1e9; mx = 0
				for (i = 1; i <= n; i++) { if (r[i] < mn) mn = r[i]; if (r[i] > mx) mx = r[i] }
				sub(/^SELECT /, "", sql)
				printf "%-42s %10.1f %10.1f %7.2fx %.2f..%.2f\n", sql, median(b, n), median(w, n), median(r, n), mn, mx
			}' "$RAW"
	done
done
