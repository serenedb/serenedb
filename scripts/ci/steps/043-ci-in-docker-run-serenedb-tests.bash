#!/bin/bash

# Run unit tests
if ! docker run --rm \
	--ulimit core=-1 \
	--ulimit nofile=16384:16384 \
	--cap-add=SYS_PTRACE \
	--privileged \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e BUILD_DIR="${BUILD_DIR}" \
	-v "${WORKSPACE}:/serenedb" \
	-v "${GTEST_PARALLEL_CACHE_DIR:-/tmp/gtest-parallel-cache}:/root/.cache" \
	-v /etc/passwd:/etc/passwd:ro \
	-v /etc/group:/etc/group:ro \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb
    sysctl -w kernel.core_pattern=/serenedb/cores/serenedb-tests-%e.%p.%h.%t
    LANG="en_US" ./scripts/gtest-parallel/gtest-parallel ./${BUILD_DIR}/bin/serenedb-tests* 2>&1 | tee -a /serenedb/serenedb-tests.log
  '; then
	echo "UNIT_TESTS=FAILED"
	exit 123
fi
echo "UNIT_TESTS=PASSED"
