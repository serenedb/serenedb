#!/usr/bin/env bash
# COPY throughput bench -- binary vs csv, export (TO STDOUT) and ingest
# (FROM STDIN), reported in MB/s. Starts its own serened on a free port, builds
# a representative table, times each COPY (best of N), tears everything down.
#
# Usage:
#   tests/bench/copy_throughput.sh [--serened PATH] [--rows N] [--iters K] [--port P]
# Defaults: serened=./build_perf/bin/serened (RelWithDebInfo -- use a perf build,
# never ./build which has debug asserts), rows=3000000, iters=3, port=7870.
set -euo pipefail

SERENED="${SERENED:-./build_perf/bin/serened}"
ROWS=3000000
ITERS=3
PORT=7870
while [ $# -gt 0 ]; do
  case "$1" in
    --serened) SERENED="$2"; shift 2;;
    --rows)    ROWS="$2";    shift 2;;
    --iters)   ITERS="$2";   shift 2;;
    --port)    PORT="$2";    shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

[ -x "$SERENED" ] || { echo "serened not found/executable: $SERENED" >&2; exit 1; }

DATA="$(mktemp -d /tmp/copybench_XXXX)"
BIN="$(mktemp /tmp/copybench_XXXX.bin)"
CSV="$(mktemp /tmp/copybench_XXXX.csv)"
LOG="$(mktemp /tmp/copybench_XXXX.log)"
cleanup() { [ -n "${SRV:-}" ] && kill -9 "$SRV" 2>/dev/null || true; rm -rf "$DATA" "$BIN" "$CSV" "$LOG"; }
trap cleanup EXIT

"$SERENED" "$DATA" --network_pg_endpoint="tcp://127.0.0.1:$PORT" >"$LOG" 2>&1 &
SRV=$!
q() { psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 -tA "$@"; }
for _ in $(seq 1 80); do q -c 'SELECT 1' >/dev/null 2>&1 && break; sleep 0.25; done

echo "serened=$SERENED  rows=$ROWS  iters=$ITERS  port=$PORT"
q -c "CREATE TABLE src AS
      SELECT g::int i4, (g::bigint*1000000) i8, (g*0.5)::float8 f8, ('row_'||g) t
      FROM generate_series(1, $ROWS) g" >/dev/null
echo "table: $(q -c 'SELECT count(*) FROM src') rows"

# best wall time (ms) over ITERS runs of a command
best_ms() {
  local b=99999999 s e ms
  for _ in $(seq 1 "$ITERS"); do
    s=$(date +%s%N); eval "$1" >/dev/null 2>&1; e=$(date +%s%N)
    ms=$(( (e - s) / 1000000 )); [ "$ms" -lt "$b" ] && b=$ms
  done
  echo "$b"
}
mbps() { awk -v b="$1" -v ms="$2" 'BEGIN{ if(ms<=0)ms=1; printf "%.1f", (b/1048576.0)/(ms/1000.0) }'; }

# stage files for the ingest direction
q -c "COPY (SELECT * FROM src) TO STDOUT (FORMAT binary)" > "$BIN"
q -c "COPY (SELECT * FROM src) TO STDOUT (FORMAT csv)"    > "$CSV"
BIN_SZ=$(stat -c%s "$BIN"); CSV_SZ=$(stat -c%s "$CSV")

printf '\n%-22s %12s %12s %12s\n' "case" "bytes" "best_ms" "MB/s"
printf '%-22s %12s %12s %12s\n' "----" "-----" "-------" "----"

# EXPORT: COPY ... TO STDOUT piped to wc (consume without disk)
ms=$(best_ms "q -c \"COPY (SELECT * FROM src) TO STDOUT (FORMAT binary)\" | wc -c")
printf '%-22s %12d %12d %12s\n' "export binary" "$BIN_SZ" "$ms" "$(mbps "$BIN_SZ" "$ms")"
ms=$(best_ms "q -c \"COPY (SELECT * FROM src) TO STDOUT (FORMAT csv)\" | wc -c")
printf '%-22s %12d %12d %12s\n' "export csv" "$CSV_SZ" "$ms" "$(mbps "$CSV_SZ" "$ms")"

# INGEST: COPY ... FROM STDIN < file, fresh table each run
ms=$(best_ms "q -c 'DROP TABLE IF EXISTS d; CREATE TABLE d(i4 int,i8 bigint,f8 double,t text)'; q -c 'COPY d FROM STDIN (FORMAT binary)' < '$BIN'")
printf '%-22s %12d %12d %12s\n' "ingest binary" "$BIN_SZ" "$ms" "$(mbps "$BIN_SZ" "$ms")"
ms=$(best_ms "q -c 'DROP TABLE IF EXISTS d; CREATE TABLE d(i4 int,i8 bigint,f8 double,t text)'; q -c 'COPY d FROM STDIN (FORMAT csv)' < '$CSV'")
printf '%-22s %12d %12d %12s\n' "ingest csv" "$CSV_SZ" "$ms" "$(mbps "$CSV_SZ" "$ms")"

# --- binary SELECT (DataRow) vs binary COPY TO STDOUT (CopyData), same client
# (psycopg3 -- psql can't request binary result format). Both now go through the
# SAME wire collector, so this isolates the framing cost (per-row DataRow header
# vs one CopyData frame per chunk). Both stream from the wire (bounded memory).
if python3 -c 'import psycopg' >/dev/null 2>&1; then
  echo
  echo "binary SELECT vs binary COPY TO STDOUT (psycopg3, both via the collector):"
  python3 - "$PORT" "$ITERS" "$BIN_SZ" <<'PY'
import sys, time, psycopg
port, iters, bin_sz = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
conn = psycopg.connect(host="127.0.0.1", port=port, dbname="postgres",
                       user="postgres", autocommit=True)
def best(fn):
    b = 1e18
    for _ in range(iters):
        s = time.perf_counter(); fn(); b = min(b, time.perf_counter() - s)
    return b
def sel():
    with conn.cursor(binary=True) as cur:
        for _ in cur.stream("SELECT * FROM src"):
            pass
def cpy():
    with conn.cursor() as cur:
        with cur.copy("COPY (SELECT * FROM src) TO STDOUT (FORMAT binary)") as cp:
            for _ in cp:
                pass
def mbps(t):
    return (bin_sz / 1048576.0) / t if t > 0 else 0.0
ts, tc = best(sel), best(cpy)
print(f"  {'select binary (DataRow)':<24} {int(ts*1000):>8} ms   {mbps(ts):>8.1f} MB/s")
print(f"  {'copy   binary (CopyData)':<24} {int(tc*1000):>8} ms   {mbps(tc):>8.1f} MB/s")
print("  note: SELECT decodes every row into client objects (psycopg); COPY streams")
print("        raw bytes. Server-side serialization is the same wire collector for")
print("        both -- the gap here is client-side row materialization, which is why")
print("        binary COPY is the bulk-extract/federation path.")
PY
else
  echo "(psycopg3 not available -- skipping binary SELECT vs COPY comparison)"
fi

if grep -qiE 'FATAL|SIGSEGV' "$LOG"; then echo; echo "WARNING: FATAL/SIGSEGV in server log:"; grep -iE 'FATAL|SIGSEGV' "$LOG" | head; fi
