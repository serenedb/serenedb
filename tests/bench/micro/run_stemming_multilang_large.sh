#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 PATH_TO_SNOWBALL_DATA" >&2
    exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DATA_ROOT="$1"
BINARY="${SERENEDB_STEMMER_BENCH_BINARY:-$REPO_ROOT/build_perf/bin/serenedb-bench-micro-stemming}"
LIMIT="${SERENEDB_STEMMER_CORPUS_LIMIT:-50000}"
REPETITIONS="${SERENEDB_STEMMER_BENCH_REPETITIONS:-10}"
MIN_TIME="${SERENEDB_STEMMER_BENCH_MIN_TIME:-0.5s}"
OUTPUT="${SERENEDB_STEMMER_BENCH_OUT:-/private/tmp/serenedb-stem-generated-c-all-languages.json}"

if [[ ! -x "$BINARY" ]]; then
    echo "Benchmark binary is missing: $BINARY" >&2
    echo "Build it with: cmake --build build_perf --target serenedb-bench-micro-stemming -j8" >&2
    exit 2
fi

SERENEDB_STEMMER_CORPUS_ROOT="$DATA_ROOT" \
SERENEDB_STEMMER_CORPUS_LIMIT="$LIMIT" \
"$BINARY" \
    --benchmark_filter='^(Stem|StemGeneratedC)/(english|german|french|spanish|italian|finnish|hungarian|turkish|russian|greek|arabic|polish|hindi|tamil)/All$' \
    --benchmark_min_time="$MIN_TIME" \
    --benchmark_repetitions="$REPETITIONS" \
    --benchmark_enable_random_interleaving=true \
    --benchmark_out="$OUTPUT" \
    --benchmark_out_format=json

echo "Raw benchmark JSON: $OUTPUT"
