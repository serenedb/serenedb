#!/bin/bash

set -o pipefail

if cd "${WORKSPACE}" && BUILD_DIR="${BUILD_DIR}" TEST_KIND="recovery" ./tests/sqllogic/run_in_docker.sh 2>&1 | tee -a ./recovery-tests.log; then
	test_result="PASSED"
	exit_code=0
else
	test_result="FAILED"
	exit_code=123
fi

echo "RECOVERY_TESTS=${test_result}"
exit ${exit_code}
