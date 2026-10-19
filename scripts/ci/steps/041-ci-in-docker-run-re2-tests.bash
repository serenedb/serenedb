#!/bin/bash

# Run re2-tests
if ! docker run --rm \
	${SDB_DOCKER_OWNER:+--label "serenedb.owner=$SDB_DOCKER_OWNER"} \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb \
	--cap-add=SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e BUILD_DIR="${BUILD_DIR}" \
	-v "${WORKSPACE}:/serenedb" \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb
    ctest --test-dir "${BUILD_DIR}/third_party/re2" -j "$(nproc)" --output-on-failure \
      --output-junit /serenedb/out/test-results/re2-tests.xml 2>&1 | tee -a /serenedb/out/logs/re2-tests.log
  '; then
	echo "RE2_TESTS=FAILED"
	exit 123
fi
echo "RE2_TESTS=PASSED"
