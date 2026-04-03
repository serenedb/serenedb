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

exec ./run.sh \
	--test "pg/**/*.test*" \
	--junit "tests-pg" \
	--protocol both \
	--database postgres \
	$FAST_FLAG \
	"$@"
