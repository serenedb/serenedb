#!/bin/bash

# Run vpack-tests
if ! docker run --rm \
	--ulimit core=-1 \
	--ulimit nofile=16384:16384 \
	--cap-add=SYS_PTRACE \
	--privileged \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e BUILD_DIR="${BUILD_DIR}" \
	-v "${WORKSPACE}:/serenedb" \
	-v /etc/passwd:/etc/passwd:ro \
	-v /etc/group:/etc/group:ro \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb
    sysctl -w kernel.core_pattern=/serenedb/cores/vpack-tests-%e.%p.%h.%t
    ctest --test-dir "${BUILD_DIR}" --tests-regex vpack --output-junit "/serenedb/vpack-tests.xml" 2>&1 | tee -a /serenedb/vpack-tests.log
  '; then
	echo "VPACK_TESTS=FAILED"
	exit 123
fi
echo "VPACK_TESTS=PASSED"
