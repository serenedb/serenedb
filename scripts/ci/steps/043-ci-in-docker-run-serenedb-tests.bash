#!/bin/bash

# Run unit tests
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
	-v "${GTEST_PARALLEL_CACHE_DIR:-/tmp/gtest-parallel-cache}:/serenedb/.cache" \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb
    LANG="en_US" ./scripts/gtest-parallel/gtest-parallel ./${BUILD_DIR}/bin/serenedb-tests* 2>&1 | tee -a /serenedb/serenedb-tests.log
  '; then
	echo "UNIT_TESTS=FAILED"
	exit 123
fi
echo "UNIT_TESTS=PASSED"
