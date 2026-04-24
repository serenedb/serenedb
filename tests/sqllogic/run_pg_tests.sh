#!/bin/bash

# Runs PostgreSQL compatibility tests (validates our .test files against real PG).
# Usage:
#   ./run_pg_tests.sh --host localhost --single-port 5432
#   SKIP_SLOW_TESTS=true ./run_pg_tests.sh --host localhost --single-port 5432

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

FAST_FLAG=""
if [[ "${SKIP_SLOW_TESTS:-false}" == "true" ]]; then
	FAST_FLAG="--fast"
fi

# When PG_CHANGED_TESTS is set (PR diff-only mode), forward each changed file
# as a separate --test so run.sh invokes sqllogictest once per engine with the
# full file list. Otherwise run the full pg test suite.
test_args=()
if [[ -n "${PG_CHANGED_TESTS:-}" ]]; then
	while IFS= read -r test_file; do
		[[ -z "$test_file" ]] && continue
		test_args+=(--test "$test_file")
	done <<<"$PG_CHANGED_TESTS"
	if [[ ${#test_args[@]} -eq 0 ]]; then
		echo "PG_CHANGED_TESTS is set but contained no files; nothing to do." >&2
		exit 0
	fi
else
	test_args=(--test "pg/**/*.test*")
fi

exec ./run.sh \
	"${test_args[@]}" \
	--junit "tests-pg" \
	--engines "pg-wire-simple,pg-wire-extended" \
	--database postgres \
	$FAST_FLAG \
	"$@"
