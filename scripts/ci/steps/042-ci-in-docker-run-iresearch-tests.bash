#!/bin/bash

# Run iresearch-tests
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
    sysctl -w kernel.core_pattern=/serenedb/cores/iresearch-tests-%e.%p.%h.%t
    cd "${BUILD_DIR}/bin"
    export MALLOC_ARENA_MAX=1 # limit the number of arenas
    python3 ../../scripts/gtest-parallel/gtest_parallel.py ./iresearch-tests -- \
      --ires_output="xml:/serenedb/iresearch-tests.xml" 2>&1 | tee -a /serenedb/iresearch-tests.log
  '; then
	echo "IRESEARCH_TESTS=FAILED"
	exit 123
fi
echo "IRESEARCH_TESTS=PASSED"
