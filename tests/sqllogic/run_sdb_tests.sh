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

# Under ASan and TSan each of these files takes 3 to 14 minutes and alone sets
# the step's wall time; the dev and perf jobs still run them. Under TSan:
# large_values is as slow there; nested_subqueries runs queries hundreds of
# levels deep, and TSan keeps every distinct call stack it has seen until the
# process exits, so those stacks alone fill most of its 32 GB stack store and
# the server crashes once it is full. ai_ollama drives a local Ollama container;
# on arm64 inference is slow enough that ai_generate's own HTTP timeout fires.
SANITIZER_SKIP=""
if [[ "${SANITIZERS:-}" == *Address* || "${SANITIZERS:-}" == *Thread* ]]; then
	SANITIZER_SKIP='simple/planner_scaling\.test|simple/unicode/regexp_properties\.test|index/(geo|phrase|wildcard)_deferred_matrix\.gen\.test'
fi
if [[ "${SANITIZERS:-}" == *Thread* ]]; then
	SANITIZER_SKIP+='|simple/large_values\.test|simple/nested_subqueries\.test'
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
