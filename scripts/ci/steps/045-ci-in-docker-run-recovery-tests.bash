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

# It's really required only for logs, but
# I cannot explain why it isn't for sanitizers and coverage,
# so let's leave as is for a while.
docker run --rm \
	--cap-add=SYS_PTRACE \
	--privileged \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e BUILD_DIR="${BUILD_DIR}" \
	-v /etc/passwd:/etc/passwd:ro \
	-v /etc/group:/etc/group:ro \
	-v "${WORKSPACE}/logs:/logs" \
	-v "${WORKSPACE}/sanitizers:/sanitizers" \
	-v "${WORKSPACE}/${BUILD_DIR}/coverage:/coverage" \
	"${BUILD_IMAGE}" bash -c 'chown -R "$1" /logs /sanitizers /coverage && echo "Permissions set successfully"' -- "${RUNNER_ID}"

exit ${exit_code}
