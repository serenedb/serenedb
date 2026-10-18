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

# The step takes as long as its slowest file, so under ASan the files that take
# over 100 s and under TSan the ones over 200 s are skipped; the dev and perf
# jobs still run them. nested_subqueries also runs queries hundreds of levels
# deep, and TSan keeps every distinct call stack it has seen until the process
# exits, so those stacks alone fill most of its 32 GB stack store and the server
# crashes once it is full. ai_ollama drives a local Ollama container; on arm64
# inference is slow enough that ai_generate's own HTTP timeout fires.
SANITIZER_SKIP=""
if [[ "${SANITIZERS:-}" == *Address* || "${SANITIZERS:-}" == *Thread* ]]; then
	SANITIZER_SKIP='simple/(planner_scaling|nested_subqueries)\.test|simple/unicode/regexp_properties\.test|simple/temporal/(bucket_groups_alias_distinct_matrix|monotone_predicate_matrix_1)\.test|index/(geo|phrase|wildcard)_deferred_matrix\.gen\.test'
fi
if [[ "${SANITIZERS:-}" == *Thread* ]]; then
	SANITIZER_SKIP+='|simple/large_values\.test|simple/unicode/case_mapping\.test'
	if [[ "$(uname -m)" == "aarch64" ]]; then
		SANITIZER_SKIP+='|_ollama\.test_slow'
	fi
fi

# sqlite subtree is under sdb/pg/any via the any/pg symlink; --skip is a path regex.
declare -a SEL
JUNIT="tests-serenedb"
case "${SDB_SQLLOGIC_SCOPE:-all}" in
ours) SEL=(--test "sdb/**/*.test*" --skip "sqlite${SANITIZER_SKIP:+|$SANITIZER_SKIP}") ;;
sqlite) SEL=(--test "sdb/pg/any/sqlite/**/*.test*") ;;
biglake)
	SEL=(--test "sdb/**/*_iceberg.test_slow" --jobs 2)
	JUNIT="tests-serenedb-biglake"
	export ICEBERG_BACKEND=biglake
	;;
all | *)
	SEL=(--test "sdb/**/*.test*")
	[[ -n "$SANITIZER_SKIP" ]] && SEL+=(--skip "$SANITIZER_SKIP")
	;;
esac

exec ./run.sh \
	"${SEL[@]}" \
	--junit "$JUNIT" \
	--engines "pg-wire-simple,pg-wire-extended" \
	--database serenedb \
	$FAST_FLAG \
	"$@"
