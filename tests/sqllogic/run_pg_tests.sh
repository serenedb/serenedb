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

# When PG_CHANGED_TESTS is set (PR diff-only mode), run only the changed
# files.  run.sh --test accepts a single glob, so we loop over the files.
# Otherwise run the full pg test suite.
if [[ -n "${PG_CHANGED_TESTS:-}" ]]; then
	exit_code=0
	while IFS= read -r test_file; do
		[[ -z "$test_file" ]] && continue
		./run.sh \
			--test "$test_file" \
			--junit "tests-pg" \
			--engines "pg-wire-simple,pg-wire-extended" \
			--database postgres \
			$FAST_FLAG \
			"$@" || exit_code=$?
	done <<<"$PG_CHANGED_TESTS"
	exit "$exit_code"
else
	exec ./run.sh \
		--test "pg/**/*.test*" \
		--junit "tests-pg" \
		--engines "pg-wire-simple,pg-wire-extended" \
		--database postgres \
		$FAST_FLAG \
		"$@"
fi
