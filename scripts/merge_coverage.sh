#!/bin/bash
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-build_cov}"
LLVM_BIN="${LLVM_BIN:-}"
IRESEARCH_FILTER="${IRESEARCH_FILTER:-*MergeWriterTestCase*:*MergeOracle*:*MergeCharacterization*:*MergeGolden*:*MergeColumns*:*MergeNorms*:*MergeIvf*:*ScorePruneNormMerge*:*compact*:*Compact*:*consolidat*:*Consolidat*:*Adopt*:*adopt*:*Hnsw*:MergePlan*:ColumnSplit*:ColumnCopy*:ColCopyCheck*:ColBlockSink*:InputNorms*:*Norm*}"
SERVER_FILTER="${SERVER_FILTER:-MergeRegistry*:MergeRecorder*:AnnBuildPool*:PipelineMergeExecutorTest*}"
SQLLOGIC_TESTS="${SQLLOGIC_TESTS:-}"
SQLLOGIC_PORT="${SQLLOGIC_PORT:-7931}"

cd "$REPO"
PROFILES="$(mktemp -d "${TMPDIR:-/tmp}/merge-coverage.XXXXXX")"
trap 'rm -rf "$PROFILES"' EXIT

tool() {
	if [[ -n "$LLVM_BIN" ]]; then
		echo "$LLVM_BIN/$1"
	else
		echo "$1"
	fi
}

LLVM_PROFILE_FILE="$PROFILES/iresearch-%p.profraw" \
	"$BUILD_DIR/bin/iresearch-tests" --gtest_filter="$IRESEARCH_FILTER"
LLVM_PROFILE_FILE="$PROFILES/server-%p.profraw" \
	"$BUILD_DIR/bin/serenedb-tests" --gtest_filter="$SERVER_FILTER"

BINARIES=("$BUILD_DIR/bin/iresearch-tests" "$BUILD_DIR/bin/serenedb-tests")
if [[ -n "$SQLLOGIC_TESTS" ]]; then
	DATA="$PROFILES/data"
	mkdir -p "$DATA"
	LLVM_PROFILE_FILE="$PROFILES/serened-%p.profraw" \
		"$BUILD_DIR/bin/serened" "$DATA" --listen="postgres://127.0.0.1:$SQLLOGIC_PORT" \
		>"$PROFILES/serened.log" 2>&1 &
	SERVER=$!
	for _ in $(seq 1 120); do
		ss -ltn | grep -q ":$SQLLOGIC_PORT " && break
		sleep 0.5
	done
	ARGS=()
	for test in $SQLLOGIC_TESTS; do
		ARGS+=(--test "$test")
	done
	./tests/sqllogic/run.sh --single-port "$SQLLOGIC_PORT" "${ARGS[@]}" || true
	kill "$SERVER"
	wait "$SERVER" || true
	BINARIES+=("$BUILD_DIR/bin/serened")
fi

PROFDATA="${PROFDATA:-$PROFILES/merged.profdata}"
"$(tool llvm-profdata)" merge -sparse "$PROFILES"/*.profraw -o "$PROFDATA"
python3 scripts/merge_coverage.py --llvm-cov "$(tool llvm-cov)" \
	--profdata "$PROFDATA" --build-dir "$BUILD_DIR" \
	--allowlist scripts/merge_coverage_allowlist.txt "$@"
