#!/bin/bash

set -e

: "${BUILD_DIR:?BUILD_DIR must be set}"
: "${CORPUS_PATH:?CORPUS_PATH must be set}"
: "${IRESEARCH_LOAD_GTEST_FILTER:=LoadTest*}"

if [[ ! -s "$CORPUS_PATH" ]]; then
	echo "ERROR: corpus file missing or empty: $CORPUS_PATH" >&2
	exit 1
fi

corpus_real="$(readlink -f "$CORPUS_PATH")"

if ! docker run --rm \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb \
	--ulimit core=-1 \
	--ulimit nofile=16384:16384 \
	--cap-add=SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e BUILD_DIR="${BUILD_DIR}" \
	-e CORPUS_PATH="/corpus/corpus.json" \
	-e IRESEARCH_LOAD_GTEST_FILTER="${IRESEARCH_LOAD_GTEST_FILTER}" \
	-v "${WORKSPACE}:/serenedb" \
	-v "${corpus_real}:/corpus/corpus.json:ro" \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    mkdir -p /serenedb/out/test-results /serenedb/out/logs
    cd /serenedb/${BUILD_DIR}/bin
    export MALLOC_ARENA_MAX=1
    ./iresearch-load-tests \
      --gtest_filter="${IRESEARCH_LOAD_GTEST_FILTER}" \
      --gtest_output="xml:/serenedb/out/test-results/iresearch-load-tests.xml" \
      2>&1 | tee -a /serenedb/out/logs/iresearch-load-tests.log
  '; then
	echo "IRESEARCH_LOAD_TESTS=FAILED"
	exit 123
fi
echo "IRESEARCH_LOAD_TESTS=PASSED"
