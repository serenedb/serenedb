#!/bin/bash

set -o pipefail

if cd "${WORKSPACE}" && BUILD_DIR="${BUILD_DIR}" LLVM_PROFILE_FILE="/serenedb/${BUILD_DIR}/coverage/profiles/sdb.%m.%p.profraw" ./tests/sqllogic/run_in_docker.sh 2>&1 | tee -a ./sqllogic-tests.log; then
	test_result="PASSED"
	exit_code=0
else
	test_result="FAILED"
	exit_code=123
fi

echo "SQLLOGIC_TESTS=${test_result}"
exit ${exit_code}
