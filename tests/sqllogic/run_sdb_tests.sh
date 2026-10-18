#!/bin/bash

# Runs SereneDB sqllogic tests.
# Usage:
#   ./run_sdb_tests.sh --host localhost --single-port 7777
#   SKIP_SLOW_TESTS=true ./run_sdb_tests.sh --host localhost --single-port 7777

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

FAST_FLAG=""
if [[ "${SKIP_SLOW_TESTS:-false}" == "true" ]]; then
	FAST_FLAG="--fast"
fi

# Under TSan: nested_subqueries runs queries hundreds of levels deep, and TSan
# keeps every distinct call stack it has seen until the process exits, so those
# stacks alone fill most of its 32 GB stack store and the server crashes once it
# is full. ai_ollama drives a local Ollama container; on arm64 inference is slow
# enough that ai_generate's own HTTP timeout fires. Neither exercises anything
# TSan-specific.
TSAN_SKIP=""
if [[ "${SANITIZERS:-}" == *Thread* ]]; then
	TSAN_SKIP='simple/nested_subqueries\.test'
	if [[ "$(uname -m)" == "aarch64" ]]; then
		TSAN_SKIP+='|_ollama\.test_slow'
	fi
fi

# sqlite subtree is under sdb/pg/any via the any/pg symlink; --skip is a path regex.
declare -a SEL
JUNIT="tests-serenedb"
case "${SDB_SQLLOGIC_SCOPE:-all}" in
ours) SEL=(--test "sdb/**/*.test*" --skip "sqlite${TSAN_SKIP:+|$TSAN_SKIP}") ;;
sqlite) SEL=(--test "sdb/pg/any/sqlite/**/*.test*") ;;
biglake)
	SEL=(--test "sdb/**/*_iceberg.test_slow" --jobs 2)
	JUNIT="tests-serenedb-biglake"
	export ICEBERG_BACKEND=biglake
	;;
all | *)
	SEL=(--test "sdb/**/*.test*")
	[[ -n "$TSAN_SKIP" ]] && SEL+=(--skip "$TSAN_SKIP")
	;;
esac

exec ./run.sh \
	"${SEL[@]}" \
	--junit "$JUNIT" \
	--engines "pg-wire-simple,pg-wire-extended" \
	--database serenedb \
	$FAST_FLAG \
	"$@"
