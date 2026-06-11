#!/bin/bash

set -o pipefail

if cd "${WORKSPACE}" && BUILD_DIR="${BUILD_DIR}" ./tests/drivers/run_in_docker.sh 2>&1 | tee -a ./out/logs/drivers-tests.log; then
	test_result="PASSED"
	exit_code=0
else
	test_result="FAILED"
	exit_code=123
fi

echo "DRIVER_TESTS=${test_result}"
exit ${exit_code}
