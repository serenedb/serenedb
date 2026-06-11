#!/bin/bash
#
# Runs the vendored DuckDB-extension test suites (currently just
# postgres_scanner; avro/httpfs follow in later phases).
# Uses docker-compose.extensions.yml which co-locates postgres alongside a
# tests container that invokes tests/postgres_scanner/run.sh --use-existing.

set -o pipefail

if cd "${WORKSPACE}" && BUILD_DIR="${BUILD_DIR}" TEST_KIND="extensions" ./tests/sqllogic/run_in_docker.sh 2>&1 | tee -a ./out/logs/extension-tests.log; then
	test_result="PASSED"
	exit_code=0
else
	test_result="FAILED"
	exit_code=123
fi

echo "EXTENSION_TESTS=${test_result}"
exit ${exit_code}
