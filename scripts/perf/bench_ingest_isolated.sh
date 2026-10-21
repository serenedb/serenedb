#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat >&2 <<'EOF'
Usage: sudo PERF_PYTHON=... OTEL_BENCH_DATA_DIR=... \
         bash scripts/perf/bench_ingest_isolated.sh MAIN_BIN CURRENT_BIN

Runs both ingest benchmarks on cpus reserved exclusively for them and prints
the results as markdown tables (also saved to $PERF_OUT/results.md):
  1. bench_ingest_vs_main.sh: ES _bulk and OTLP, MAIN_BIN vs CURRENT_BIN, in a
     cpuset partition made by run_bench_isolated.sh.
  2. otel_bench/run.py: SereneDB (CURRENT_BIN) vs ClickHouse, Elasticsearch,
     Victoria* and Quickwit. The engines run in an exclusive systemd slice
     (containers via --cgroup-parent, serened via systemd-run --scope), the
     client in a second exclusive partition on the client cpus.

Environment:
  PERF_PYTHON          python with psycopg2, pyarrow and opentelemetry-proto (required)
  OTEL_BENCH_DATA_DIR  data prepared by otel_bench/prepare.py (required)
  PERF_SERVER_CORES    cpus for the servers (default 8-15,24-31)
  PERF_LOAD_CORES      cpus for the clients (default 3-7,19-23)
  PERF_TMPDIR          serened datadirs of step 1 (default /dev/shm)
  PERF_ROUNDS          step 1 rounds, best wins (default 3)
  OTEL_ENGINES         step 2 engines, comma separated (default all)
  OTEL_SIGNALS         step 2 signals (default "logs traces metrics")
  OTEL_SIZE            step 2 data size per signal (default 1g)
  OTEL_CONCURRENCY     step 2 connections (default 8)
  OTEL_ISOLATION       partition (default) or pin: cpu affinity only, no exclusivity
  PERF_OUT             output directory (default scripts/perf/results/ingest-<time>)
  SKIP_INGEST=1, SKIP_OTEL=1 skip a step
EOF
	exit 2
}

[[ $# -eq 2 ]] || usage
if [[ $EUID -ne 0 ]]; then
	echo "must run as root: sudo bash $0 MAIN_BIN CURRENT_BIN" >&2
	exit 1
fi
: "${PERF_PYTHON:?set PERF_PYTHON to a python with psycopg2, pyarrow and opentelemetry-proto}"
: "${OTEL_BENCH_DATA_DIR:?set OTEL_BENCH_DATA_DIR to the otel_bench data directory}"

MAIN_BIN=$(realpath "$1")
CURRENT_BIN=$(realpath "$2")
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
RUN_USER=${SUDO_USER:-$(id -un)}
SERVER_CORES=${PERF_SERVER_CORES:-8-15,24-31}
LOAD_CORES=${PERF_LOAD_CORES:-3-7,19-23}
SIGNALS=${OTEL_SIGNALS:-logs traces metrics}
ISOLATION=${OTEL_ISOLATION:-partition}
OUT=${PERF_OUT:-$ROOT/scripts/perf/results/ingest-$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"
chown "$RUN_USER" "$OUT"

SLICE=otelbench.slice
SLICE_CG=/sys/fs/cgroup/$SLICE
CLIENT_CG=/sys/fs/cgroup/otelbench-client

release() {
	[[ -d $1 ]] || return 0
	echo member >"$1/cpuset.cpus.partition" 2>/dev/null || true
	echo >"$1/cpuset.cpus.exclusive" 2>/dev/null || true
}

cleanup() {
	echo $$ >/sys/fs/cgroup/cgroup.procs 2>/dev/null || true
	docker ps -aq --filter name=otel-bench- | xargs -r docker rm -f >/dev/null 2>&1 || true
	release "$CLIENT_CG"
	rmdir "$CLIENT_CG" 2>/dev/null || true
	release "$SLICE_CG"
	systemctl stop otelbench-hold.service >/dev/null 2>&1 || true
	chown -R "$RUN_USER" "$OUT" 2>/dev/null || true
}
trap cleanup EXIT

partition() {
	echo "$2" >"$1/cpuset.cpus"
	cat /sys/devices/system/node/online >"$1/cpuset.mems"
	echo "$2" >"$1/cpuset.cpus.exclusive"
	echo root >"$1/cpuset.cpus.partition"
	local state
	state=$(cat "$1/cpuset.cpus.partition")
	if [[ $state != root ]]; then
		echo "ERROR: $1 is '$state', not an exclusive partition" >&2
		exit 1
	fi
	echo "partition $1: cpus $(cat "$1/cpuset.cpus.effective")" >&2
}

if [[ -z ${SKIP_INGEST:-} ]]; then
	echo "== ES/OTLP ingest, main vs current" >&2
	BENCH_CORES="$SERVER_CORES,$LOAD_CORES" PERF_SERVER_CORES=$SERVER_CORES \
		PERF_LOAD_CORES=$LOAD_CORES PERF_TMPDIR=${PERF_TMPDIR:-/dev/shm} \
		PERF_PYTHON=$PERF_PYTHON PERF_OUT=$OUT/ingest \
		bash "$ROOT/scripts/perf/run_bench_isolated.sh" \
		bash "$ROOT/scripts/perf/bench_ingest_vs_main.sh" "$MAIN_BIN" "$CURRENT_BIN" \
		>"$OUT/ingest.log" 2>&1 || {
		tail -20 "$OUT/ingest.log" >&2
		exit 1
	}
	grep '^|' "$OUT/ingest.log" >"$OUT/ingest.md"
fi

if [[ -z ${SKIP_OTEL:-} ]]; then
	echo "== OTel engines ($ISOLATION)" >&2
	engine_env=()
	client=()
	if [[ $ISOLATION == partition ]]; then
		systemctl stop otelbench-hold.service >/dev/null 2>&1 || true
		systemd-run --quiet --unit=otelbench-hold --slice=$SLICE sleep infinity
		for _ in $(seq 100); do
			[[ -d $SLICE_CG ]] && break
			sleep 0.1
		done
		partition "$SLICE_CG" "$SERVER_CORES"
		mkdir -p "$CLIENT_CG"
		partition "$CLIENT_CG" "$LOAD_CORES"
		echo $$ >"$CLIENT_CG/cgroup.procs"
		engine_env=(OTEL_BENCH_SLICE=$SLICE)
	else
		engine_env=(OTEL_BENCH_SERVER_CPUS=$SERVER_CORES)
		client=(taskset -c "$LOAD_CORES")
	fi
	engines=()
	if [[ -n ${OTEL_ENGINES:-} ]]; then
		engines=(--engines "$OTEL_ENGINES")
	fi
	for signal in $SIGNALS; do
		echo "   $signal" >&2
		env "${engine_env[@]}" OTEL_BENCH_DATA_DIR="$OTEL_BENCH_DATA_DIR" \
			"${client[@]}" "$PERF_PYTHON" "$ROOT/scripts/perf/otel_bench/run.py" \
			"${engines[@]}" --signal "$signal" --size "${OTEL_SIZE:-1g}" \
			--concurrency "${OTEL_CONCURRENCY:-8}" --bin "$CURRENT_BIN" \
			--json "$OUT/otel-$signal.json" >"$OUT/otel-$signal.log" 2>&1 ||
			echo "   $signal failed, see $OUT/otel-$signal.log" >&2
	done
	echo $$ >/sys/fs/cgroup/cgroup.procs
	python3 "$ROOT/scripts/perf/otel_bench/table.py" "$OUT" >"$OUT/otel.md"
fi

{
	if [[ -z ${SKIP_INGEST:-} ]]; then
		echo "Main: $MAIN_BIN"
	fi
	echo "Current: $CURRENT_BIN"
	echo "Server cpus $SERVER_CORES, client cpus $LOAD_CORES"
	if [[ -s $OUT/ingest.md ]]; then
		echo
		echo "### ES/OTLP ingest, main vs current"
		echo
		cat "$OUT/ingest.md"
	fi
	if [[ -s $OUT/otel.md ]]; then
		echo
		echo "### OTel engines, ${OTEL_CONCURRENCY:-8} connections, ${OTEL_SIZE:-1g} per signal, pb+gzip"
		echo
		cat "$OUT/otel.md"
	fi
} | tee "$OUT/results.md"
