#!/usr/bin/env bash
# Measures on-disk size of an HNSW index after a known-size dataset.
# Run separately on `main` and on this branch; compare totals.
#
# Usage:
#   SERENED=./build/bin/serened ROWS=100000 DIM=128 \
#       scripts/perf/hnsw_index_size.sh
#
# Env vars:
#   SERENED    Path to the serened binary (default ./build/bin/serened).
#   PORT       PG-wire port (default 6262).
#   DATA_DIR   Data dir (default /tmp/hnsw_size_$$).
#   ROWS       Row count (default 100000).
#   DIM        Vector dimension (default 128).
#   M          HNSW M parameter (default 16).
#   EF         efConstruction (default 64).
#   METRIC     Distance metric: l2 / cosine / l1 / ip (default l2).
#   SEED       PRNG seed for the random vector data (default 42).

set -euo pipefail

PORT="${PORT:-6262}"
DATA_DIR="${DATA_DIR:-/tmp/hnsw_size_$$}"
# Default to a release build (RelWithDebInfo / no sanitizers) so the
# benchmark isn't bottlenecked on ASan or stdlib debug checks. Override
# with SERENED=... if your release binary lives elsewhere.
SERENED="${SERENED:-./build_perf/bin/serened}"
DIM="${DIM:-128}"
ROWS="${ROWS:-10000}"
M="${M:-16}"
EF="${EF:-64}"
METRIC="${METRIC:-l2}"
SEED="${SEED:-42}"

if [ ! -x "$SERENED" ]; then
	echo "ERROR: serened not found at $SERENED" >&2
	exit 1
fi
if ! command -v psql >/dev/null; then
	echo "ERROR: psql is required" >&2
	exit 1
fi

rm -rf "$DATA_DIR"

SERENED_PID=""
cleanup() {
	if [ -n "$SERENED_PID" ] && kill -0 "$SERENED_PID" 2>/dev/null; then
		kill -INT "$SERENED_PID" 2>/dev/null || true
		wait "$SERENED_PID" 2>/dev/null || true
	fi
}
trap cleanup EXIT

# Start serened on a private port + data dir.
"$SERENED" "$DATA_DIR" \
	--server_endpoints "pgsql+tcp://127.0.0.1:${PORT}" \
	SERENED_PID=$!

PG_ADMIN=(psql -v ON_ERROR_STOP=1 -X -q
	"host=127.0.0.1 port=${PORT} dbname=postgres user=postgres")
PSQL=(psql -v ON_ERROR_STOP=1 -X -q
	"host=127.0.0.1 port=${PORT} dbname=bench user=postgres")

# Wait until the admin DB accepts queries (serened's `postgres` admin DB
# comes up before user DBs are creatable).
for _ in $(seq 1 60); do
	if "${PG_ADMIN[@]}" -c 'SELECT 1' >/dev/null 2>&1; then break; fi
	sleep 0.5
done
"${PG_ADMIN[@]}" -c 'SELECT 1' >/dev/null

# Fresh database for the benchmark.
"${PG_ADMIN[@]}" -c 'DROP DATABASE IF EXISTS bench;' >/dev/null 2>&1 || true
"${PG_ADMIN[@]}" -c 'CREATE DATABASE bench;' >/dev/null

# Build the dataset + index. Vectors are uniform random floats with a
# fixed seed so both branches see the same bytes (compression should
# differ only because of codec choices, not data).
"${PSQL[@]}" <<SQL
CREATE TABLE bench (id BIGINT PRIMARY KEY, vec FLOAT[${DIM}]);

SELECT setseed(${SEED}::float / 1000000);

-- Simulate realistic quantized embeddings: values in [-1, 1] with 0.01
-- precision (200 distinct values per dimension). This fits ALP's
-- "integer * 10^k" model -- ALP compresses well, decode is fast.
-- Random uniform [0, 1) floats are the worst case for ALPRD (full
-- mantissa entropy, slow bit-pack decode). Real-world FP32 embeddings
-- are usually low-precision quantized -- this matches that.
INSERT INTO bench
SELECT i,
  [((random() * 200)::BIGINT - 100)::FLOAT / 100.0
   FOR _ IN range(${DIM})]::FLOAT[${DIM}]
FROM range(${ROWS}) AS t(i);

CREATE INDEX bench_ann ON bench
USING inverted(vec hnsw (metric = '${METRIC}', m = ${M}, ef_construction = ${EF}));

CHECKPOINT;
SQL

# Let any background flush settle, then stop the server so file sizes
# don't shift mid-measurement.
sleep 3
kill -INT "$SERENED_PID" 2>/dev/null || true
wait "$SERENED_PID" 2>/dev/null || true
SERENED_PID=""

bytes_to_human() {
	awk -v b="$1" 'BEGIN {
    units = "B KB MB GB TB"; split(units, u, " ");
    for (i = 1; i <= 5 && b >= 1024; ++i) b /= 1024;
    printf "%.2f %s\n", b, u[i]
  }'
}

echo
echo "=== HNSW index size benchmark ==="
echo "Branch:   $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
echo "Commit:   $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
echo "Rows:     ${ROWS}"
echo "Dim:      ${DIM}"
echo "M:        ${M}"
echo "EF:       ${EF}"
echo "Metric:   ${METRIC}"
echo "Data dir: ${DATA_DIR}"
echo

echo "--- Total data dir size ---"
TOTAL=$(du -sb "$DATA_DIR" | awk '{print $1}')
echo "$TOTAL bytes ($(bytes_to_human "$TOTAL"))"

echo
echo "--- Per-extension breakdown (sorted by total size) ---"
find "$DATA_DIR" -type f -printf '%s %p\n' |
	awk '{
    n = split($2, parts, "/");
    name = parts[n];
    if (match(name, /\.[A-Za-z0-9_]+$/)) {
      ext = substr(name, RSTART);
    } else { ext = "(none)"; }
    sum[ext] += $1; cnt[ext] += 1
  }
  END {
    for (e in sum) printf "%d %d %s\n", sum[e], cnt[e], e
  }' |
	sort -rn |
	awk '{
    sum=$1; cnt=$2; ext=$3;
    h = sum;
    units = "B KB MB GB TB"; split(units, u, " ");
    for (i = 1; i <= 5 && h >= 1024; ++i) h /= 1024;
    printf "  %-12s %5d files  %12d bytes  (%6.2f %s)\n", ext, cnt, sum, h, u[i]
  }'

echo
echo "--- Largest 20 files ---"
find "$DATA_DIR" -type f -printf '%s %p\n' | sort -rn | head -20 |
	awk '{
    h = $1;
    units = "B KB MB GB TB"; split(units, u, " ");
    for (i = 1; i <= 5 && h >= 1024; ++i) h /= 1024;
    printf "  %12d (%6.2f %s)  %s\n", $1, h, u[i], $2
  }'

echo
echo "Note: re-run with the same SEED + ROWS + DIM on the other branch"
echo "to compare apples-to-apples."
