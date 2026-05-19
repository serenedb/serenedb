#!/bin/bash
# Run iresearch-tests SearchBenchTest filter against the host-cached corpus.
# Expects: BUILD_DIR, WORKSPACE, BUILD_IMAGE, CORPUS_PATH
# Optional: SEARCHBENCH_GTEST_FILTER (default: SearchBenchTest.WikiSmall)

set -e

: "${BUILD_DIR:?BUILD_DIR must be set}"
: "${CORPUS_PATH:?CORPUS_PATH must be set}"
: "${SEARCHBENCH_GTEST_FILTER:=SearchBenchTest*}"

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
	-e CORPUS_PATH="/searchbench/corpus.json" \
	-e SEARCHBENCH_GTEST_FILTER="${SEARCHBENCH_GTEST_FILTER}" \
	-v "${WORKSPACE}:/serenedb" \
	-v "${corpus_real}:/searchbench/corpus.json:ro" \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb/${BUILD_DIR}/bin
    export MALLOC_ARENA_MAX=1
    ./iresearch-tests \
      --gtest_filter="${SEARCHBENCH_GTEST_FILTER}" \
      --gtest_output="xml:/serenedb/searchbench-tests.xml" \
      2>&1 | tee -a /serenedb/searchbench-tests.log
  '; then
	echo "SEARCHBENCH_TESTS=FAILED"
	exit 123
fi
echo "SEARCHBENCH_TESTS=PASSED"
