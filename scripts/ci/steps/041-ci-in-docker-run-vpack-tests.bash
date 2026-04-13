#!/bin/bash

# Run vpack-tests
if ! docker run --rm \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb \
	--ulimit core=-1 \
	--ulimit nofile=16384:16384 \
	--cap-add=SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e BUILD_DIR="${BUILD_DIR}" \
	-v "${WORKSPACE}:/serenedb" \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb
    ctest --test-dir "${BUILD_DIR}" --tests-regex vpack --output-junit "/serenedb/vpack-tests.xml" 2>&1 | tee -a /serenedb/vpack-tests.log
  '; then
	echo "VPACK_TESTS=FAILED"
	exit 123
fi
echo "VPACK_TESTS=PASSED"
