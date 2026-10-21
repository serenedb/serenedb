#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat >&2 <<'EOF'
Usage: scripts/perf/bench_ingest_vs_main.sh MAIN_BIN CURRENT_BIN

Runs scripts/perf/es_ingest.py and scripts/perf/otel_ingest.py (sustained mode)
against both serened binaries and prints the "versus main" table.

Environment:
  PERF_SERVER_CORES  taskset CPU list for serened (default: unpinned)
  PERF_LOAD_CORES    taskset CPU list for the curl/psql clients (default: unpinned)
  PERF_CONCURRENCY   connections to measure (default: 1,8)
  PERF_ES_TOTAL      NDJSON per ES run (default: 512m)
  PERF_ES_CHUNK      ES _bulk request size (default: 8m)
  PERF_OTLP_TOTAL    OTLP/JSON per OTLP run (default: 1g)
  PERF_OTLP_CHUNK    OTLP request size (default: 32m)
  PERF_PYTHON        python with psycopg2 (default: python3)
  PERF_OUT           directory for raw outputs (default: a new temp dir)
  PERF_TMPDIR        where serened datadirs go, e.g. /dev/shm to take disk syncs out
  PERF_ROUNDS        rounds; main/current alternate order per round, best run wins (default: 3)

Isolated (exclusive cpuset partition, needs sudo):
  sudo BENCH_PHYS_CORES=12 PERF_SERVER_CORES=8-15,24-31 PERF_LOAD_CORES=3-7,19-23 PERF_TMPDIR=/dev/shm \
    bash scripts/perf/run_bench_isolated.sh \
    scripts/perf/bench_ingest_vs_main.sh MAIN_BIN CURRENT_BIN
EOF
	exit 2
}

[[ $# -eq 2 ]] || usage
MAIN_BIN=$(realpath "$1")
CURRENT_BIN=$(realpath "$2")
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
CONCURRENCY=${PERF_CONCURRENCY:-1,8}
ES_TOTAL=${PERF_ES_TOTAL:-512m}
ES_CHUNK=${PERF_ES_CHUNK:-8m}
OTLP_TOTAL=${PERF_OTLP_TOTAL:-1g}
OTLP_CHUNK=${PERF_OTLP_CHUNK:-32m}
PYTHON=${PERF_PYTHON:-python3}
OUT=${PERF_OUT:-$(mktemp -d -t ingest-vs-main-XXXX)}
ROUNDS=${PERF_ROUNDS:-3}
mkdir -p "$OUT"

if [[ -n "${PERF_TMPDIR:-}" ]]; then
	export TMPDIR=$PERF_TMPDIR
fi

client=()
if [[ -n "${PERF_LOAD_CORES:-}" ]]; then
	client=(taskset -c "$PERF_LOAD_CORES")
fi

wrap() {
	local label=$1 bin=$2 wrapper="$OUT/serened-$1"
	if [[ -n "${PERF_SERVER_CORES:-}" ]]; then
		printf '#!/bin/sh\nexec taskset -c %s %s "$@"\n' "$PERF_SERVER_CORES" "$bin" >"$wrapper"
	else
		printf '#!/bin/sh\nexec %s "$@"\n' "$bin" >"$wrapper"
	fi
	chmod +x "$wrapper"
	echo "$wrapper"
}

echo "raw outputs: $OUT" >&2
echo "server cores: ${PERF_SERVER_CORES:-unpinned}, client cores: ${PERF_LOAD_CORES:-unpinned}" >&2
cd "$ROOT"
main_wrapper=$(wrap main "$MAIN_BIN")
current_wrapper=$(wrap current "$CURRENT_BIN")
for round in $(seq "$ROUNDS"); do
	order=(main current)
	((round % 2 == 0)) && order=(current main)
	for api in es otlp; do
		for path in curl gzip psql; do
			for label in "${order[@]}"; do
				wrapper=$main_wrapper
				[[ $label == current ]] && wrapper=$current_wrapper
				echo "== round $round: $api $path $label" >&2
				run=$OUT/$api-$label-$path-r$round
				script=scripts/perf/es_ingest.py
				sizes=(--total "$ES_TOTAL" --chunk "$ES_CHUNK")
				if [[ $api == otlp ]]; then
					script=scripts/perf/otel_ingest.py
					sizes=(--total "$OTLP_TOTAL" --chunk "$OTLP_CHUNK")
				fi
				if ! "${client[@]}" "$PYTHON" "$script" --bin "$wrapper" --paths "$path" \
					"${sizes[@]}" --concurrency "$CONCURRENCY" >"$run.txt" 2>"$run.log"; then
					echo "   failed, see $run.log" >&2
				fi
			done
		done
	done
done

"$PYTHON" - "$OUT" <<'EOF'
import collections
import pathlib
import re
import sys

out = pathlib.Path(sys.argv[1])
runs = collections.defaultdict(list)
for txt in sorted(out.glob("*-r*.txt")):
    api, label, path, _ = txt.stem.split("-")
    for line in txt.read_text().splitlines():
        m = re.match(r"\s*(\d+)\s+(\S+)\s+\d+\s+([\d.]+)\s", line)
        if m and m[2] == path:
            runs[(api, label, int(m[1]), path)].append(float(m[3]))

def best(values):
    return f"{max(values):.1f}" if values else "failed"


def each(values):
    return "/".join(f"{x:.0f}" for x in values) or "-"


print("| api | connections | client | main MB/s | current MB/s | speedup | runs main / current |")
print("|---|---|---|---|---|---|---|")
for api, name in (("es", "ES _bulk"), ("otlp", "OTLP")):
    for conc in sorted({k[2] for k in runs}):
        for path in ("curl", "gzip", "psql"):
            m = runs.get((api, "main", conc, path), [])
            c = runs.get((api, "current", conc, path), [])
            if not m and not c:
                continue
            speedup = f"{max(c) / max(m):.2f}x" if m and c else "-"
            print(f"| {name} | {conc} | {path} | {best(m)} | {best(c)} | {speedup} | "
                  f"{each(m)} vs {each(c)} |")
EOF
