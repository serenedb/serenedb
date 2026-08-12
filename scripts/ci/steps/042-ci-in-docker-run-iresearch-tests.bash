#!/bin/bash

# Run iresearch-tests

# gtest-parallel caches timings under XDG_CACHE_HOME; mount it at a top-level
# path -- nested under /serenedb the daemon creates a root-owned host dir.
if ! docker run --rm \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb \
	-e XDG_CACHE_HOME=/gtest-cache \
	--ulimit core=-1 \
	--ulimit nofile=16384:16384 \
	--cap-add=SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e BUILD_DIR="${BUILD_DIR}" \
	-v "${WORKSPACE}:/serenedb" \
	-v "${GTEST_PARALLEL_CACHE_DIR:-/tmp/gtest-parallel-cache}:/gtest-cache" \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb/${BUILD_DIR}/bin
    export MALLOC_ARENA_MAX=1 # limit the number of arenas
    python3 ../../scripts/gtest-parallel/gtest_parallel.py ./iresearch-tests -- \
      --ires_output="xml:/serenedb/out/test-results/iresearch-tests.xml" 2>&1 | tee -a /serenedb/out/logs/iresearch-tests.log
  '; then
	echo "IRESEARCH_TESTS=FAILED"
	exit 123
fi
echo "IRESEARCH_TESTS=PASSED"
