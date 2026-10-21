#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat >&2 <<'EOF'
Usage: sudo bash scripts/perf/run_bench_isolated.sh \
         bash scripts/perf/bench_copy_stdin.sh [SERENED_BIN...] [-- bench_copy_stdin.py args...]

COPY FROM STDIN throughput of one or more serened binaries (for example main
vs this branch), regular and search tables, text and binary, runs interleaved
across the binaries. Without a binary, build_bench_no_lto/bin/serened is used.

Environment:
  PERF_SERVER_CORES  cpus for serened (default 8-15,24-31)
  PERF_CLIENT_CORES  cpus for the client (set by run_bench_isolated.sh, default 7,23)
  PERF_PYTHON        python with psycopg 3 (default tests/drivers/python/.venv)
  PERF_PORT          first port, one per binary (default 17940)
  PERF_TMPDIR        datadirs and generated data (default /dev/shm)
  PERF_COPY_DATA     generated data, reused across runs (default $PERF_TMPDIR/sdb-copy-data)
  PERF_OUT           output directory (default scripts/perf/results/copy-stdin-<time>)
EOF
	exit 2
}

[[ ${1:-} == -h || ${1:-} == --help ]] && usage

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
BINS=()
while [[ $# -gt 0 && $1 != -- ]]; do
	BINS+=("$(realpath "$1")")
	shift
done
[[ ${1:-} == -- ]] && shift
((${#BINS[@]} > 0)) || BINS=("$ROOT/build_bench_no_lto/bin/serened")

SERVER_CORES=${PERF_SERVER_CORES:-8-15,24-31}
CLIENT_CORES=${PERF_CLIENT_CORES:-7,23}
PY=${PERF_PYTHON:-$ROOT/tests/drivers/python/.venv/bin/python}
PORT=${PERF_PORT:-17940}
TMP=${PERF_TMPDIR:-/dev/shm}
DATA=${PERF_COPY_DATA:-$TMP/sdb-copy-data}
OUT=${PERF_OUT:-$ROOT/scripts/perf/results/copy-stdin-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT" "$DATA"

PIDS=()
DBS=()
cleanup() {
	for pid in "${PIDS[@]}"; do
		kill -9 "$pid" 2>/dev/null || true
	done
	wait 2>/dev/null || true
	for db in "${DBS[@]}"; do
		rm -rf "$db"
	done
}
trap cleanup EXIT

SERVERS=()
for i in "${!BINS[@]}"; do
	bin=${BINS[$i]}
	port=$((PORT + i))
	label="$((i + 1))-$(basename "$(dirname "$(dirname "$bin")")")"
	db=$(mktemp -d "$TMP/sdb-copy-db.XXXXXX")
	DBS+=("$db")
	echo "$label: $bin on port $port, cpus $SERVER_CORES"
	taskset -c "$SERVER_CORES" "$bin" "$db" --listen="postgres://127.0.0.1:$port" >"$OUT/server-$label.log" 2>&1 &
	PIDS+=($!)
	SERVERS+=(--server "$label:$port:$!")
done
echo "client on cpus $CLIENT_CORES, results in $OUT"
taskset -c "$CLIENT_CORES" "$PY" -I "$ROOT/scripts/perf/bench_copy_stdin.py" \
	"${SERVERS[@]}" --data "$DATA" "$@" | tee "$OUT/results.md"
