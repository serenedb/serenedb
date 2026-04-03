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

exec ./run.sh \
	--test "sdb/**/*.test*" \
	--junit "tests-serenedb" \
	--engines "pg-wire-simple,pg-wire-extended" \
	--database serenedb \
	$FAST_FLAG \
	"$@"
