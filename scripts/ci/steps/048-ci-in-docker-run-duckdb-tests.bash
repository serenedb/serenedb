#!/bin/bash
#
# Runs the DuckDB-level test suites named in DUCKDB_SUITES (diff-derived by
# scripts/ci/classify-changes.sh): duckdb core's own test tree and each vendored
# extension's, slow tests included. Uses docker-compose.duckdb.yml, which
# co-locates postgres (for the postgres_scanner suite) alongside a tests
# container that invokes tests/duckdb/run.sh.

set -o pipefail

if cd "${WORKSPACE}" &&
	BUILD_DIR="${BUILD_DIR}" DUCKDB_SUITES="${DUCKDB_SUITES:-}" TEST_KIND="duckdb" \
		./tests/sqllogic/run_in_docker.sh 2>&1 | tee -a ./out/logs/duckdb-tests.log; then
	test_result="PASSED"
	exit_code=0
else
	test_result="FAILED"
	exit_code=123
fi

echo "DUCKDB_TESTS=${test_result}"
exit ${exit_code}
