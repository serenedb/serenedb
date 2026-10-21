#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat >&2 <<'EOF'
Usage: sudo PERF_PYTHON=... OTEL_BENCH_DATA_DIR=... \
         bash scripts/perf/bench_otel_serenedb_isolated.sh [SERENED_BIN]

Builds build_bench_no_lto/bin/serened from the working tree (unless
SERENED_BIN is given), then runs only SereneDB through otel_bench/run.py on
reserved cpus once per search WAL mode and prints one markdown table:
  flush1s       SDB_WAL_GROUP_COMMIT=0 SDB_WAL_SYNC_INTERVAL_MS=1000
                commit under the append mutex, flush only, fsync every 1 s
  group-flush1s SDB_WAL_SYNC_INTERVAL_MS=1000
                group commit, flush only, fsync every 1 s
  group         group commit, every commit waits for its fsync
  head          SDB_WAL_GROUP_COMMIT=0: HEAD's commit, fsync under the mutex
OTEL_MODES picks the modes (default "flush1s group-flush1s group"). Every
bench_ingest_isolated.sh variable applies (PERF_SERVER_CORES, PERF_LOAD_CORES,
OTEL_SIGNALS, OTEL_SIZE, OTEL_CONCURRENCY, OTEL_ISOLATION, PERF_OUT).
EOF
	exit 2
}

[[ $# -le 1 ]] || usage
if [[ $EUID -ne 0 ]]; then
	echo "must run as root: sudo bash $0" >&2
	exit 1
fi

HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd -- "$HERE/../.." && pwd)
RUN_USER=${SUDO_USER:-$(id -un)}
OUT=${PERF_OUT:-$HERE/results/otel-serenedb-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"
chown "$RUN_USER" "$OUT"

if [[ $# -eq 1 ]]; then
	cp "$1" "$OUT/serened"
else
	echo "== building build_bench_no_lto/bin/serened" >&2
	sudo -H -u "$RUN_USER" --preserve-env=PATH \
		ninja -C "$ROOT/build_bench_no_lto" serened >"$OUT/build.log" 2>&1 || {
		tail -20 "$OUT/build.log" >&2
		exit 1
	}
	cp "$ROOT/build_bench_no_lto/bin/serened" "$OUT/serened"
fi
BIN=$OUT/serened

sources=()
for mode in ${OTEL_MODES:-flush1s group-flush1s group}; do
	case $mode in
	flush1s)
		wal=(SDB_WAL_GROUP_COMMIT=0 SDB_WAL_SYNC_INTERVAL_MS=1000)
		label="flush, fsync 1s"
		;;
	group-flush1s)
		wal=(SDB_WAL_SYNC_INTERVAL_MS=1000)
		label="group commit, flush, fsync 1s"
		;;
	group)
		wal=()
		label="group commit, fsync per commit"
		;;
	head)
		wal=(SDB_WAL_GROUP_COMMIT=0)
		label="HEAD, fsync per commit"
		;;
	*)
		echo "unknown mode '$mode' (flush1s, group-flush1s, group, head)" >&2
		exit 2
		;;
	esac
	echo "== serenedb: $label" >&2
	env -u SDB_WAL_GROUP_COMMIT -u SDB_WAL_SYNC_INTERVAL_MS "${wal[@]}" \
		SKIP_INGEST=1 OTEL_ENGINES=serenedb PERF_OUT="$OUT/$mode" \
		bash "$HERE/bench_ingest_isolated.sh" "$BIN" "$BIN" >/dev/null
	sources+=("$label=$OUT/$mode")
done

{
	echo "SereneDB: build_bench_no_lto at $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet HEAD -- server || echo ' + working tree')"
	echo
	python3 "$HERE/otel_bench/table.py" "${sources[@]}"
} | tee "$OUT/results.md"
chown -R "$RUN_USER" "$OUT"
