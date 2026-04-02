#!/bin/bash
set -e

# Parse JSON config from BUILD_CONFIG env variable (if provided)
if [[ -n "$BUILD_CONFIG" && "$BUILD_CONFIG" != "{}" ]]; then
	eval "$(echo "$BUILD_CONFIG" | jq -r 'to_entries | .[] | "export \(.key)=\"\(.value)\""')"
fi

DOCKER_USER="$(id -u):$(id -g)"

# Release build
if [[ "${ENABLE_RELEASE_BUILD:-false}" == "true" && ("$SANITIZERS" == "None" || -z "$SANITIZERS") ]]; then
	docker run --rm \
		--user "$DOCKER_USER" \
		-e HOME=/serenedb \
		-e CCACHE_DIR=/.ccache \
		--cap-add=SYS_PTRACE \
		--security-opt seccomp=unconfined \
		--env-file ./docker.env \
		-e SDB_GTEST="${RELEASE_SDB_GTEST:-Off}" \
		-e BUILDMODE="${RELEASE_BUILDMODE:-Release}" \
		-e USE_DEBUG_INFO="${RELEASE_USE_DEBUG_INFO:-NONE}" \
		-e SDB_DEV="${RELEASE_SDB_DEV:-Off}" \
		-e SDB_FAULT_INJECTION="${RELEASE_SDB_FAULT_INJECTION:-Off}" \
		-e USE_IPO="${RELEASE_USE_IPO:-On}" \
		-e USE_COVERAGE="${RELEASE_USE_COVERAGE:-}" \
		-e ENSURE_VTUNE_SYMBOLS="${RELEASE_ENSURE_VTUNE_SYMBOLS:-Off}" \
		-e BUILD_DIR="${RELEASE_BUILD_DIR:-build}" \
		-v "${WORKSPACE}:/serenedb" \
		-v /mnt/data/.ccache:/.ccache \
		"${BUILD_IMAGE}" /serenedb/scripts/ci/build.bash serenedb tzdata
else
	echo "Release build skipped because SANITIZERS flag is set."
fi

# Build gtest
if [[ "${ENABLE_GTEST_BUILD:-false}" == "true" ]]; then
	docker run --rm \
		--user "$DOCKER_USER" \
		-e HOME=/serenedb \
		-e CCACHE_DIR=/.ccache \
		--cap-add=SYS_PTRACE \
		--security-opt seccomp=unconfined \
		--env-file ./docker.env \
		-e SDB_GTEST="${GTEST_SDB_GTEST:-On}" \
		-e BUILDMODE="${GTEST_BUILDMODE:-RelWithDebInfo}" \
		-e USE_DEBUG_INFO="${GTEST_USE_DEBUG_INFO:-COLUMNS}" \
		-e SDB_DEV="${GTEST_SDB_DEV:-On}" \
		-e SDB_FAULT_INJECTION="${GTEST_SDB_FAULT_INJECTION:-On}" \
		-e USE_IPO="${GTEST_USE_IPO:-Off}" \
		-e USE_COVERAGE="${GTEST_USE_COVERAGE:-}" \
		-e ENSURE_VTUNE_SYMBOLS="${GTEST_ENSURE_VTUNE_SYMBOLS:-Off}" \
		-e BUILD_DIR="${GTEST_BUILD_DIR:-build_gtest}" \
		-v "${WORKSPACE}:/serenedb" \
		-v /mnt/data/.ccache:/.ccache \
		"${BUILD_IMAGE}" /serenedb/scripts/ci/build.bash vpack-tests serenedb-tests_basics serenedb-tests_connector iresearch-tests fuerte-tests
fi

# Build tests
if [[ "${ENABLE_TESTS_BUILD:-false}" == "true" ]]; then
	docker run --rm \
		--user "$DOCKER_USER" \
		-e HOME=/serenedb \
		-e CCACHE_DIR=/.ccache \
		--cap-add=SYS_PTRACE \
		--security-opt seccomp=unconfined \
		--env-file ./docker.env \
		-e SDB_GTEST="${TESTS_SDB_GTEST:-Off}" \
		-e BUILDMODE="${TESTS_BUILDMODE:-RelWithDebInfo}" \
		-e USE_DEBUG_INFO="${TESTS_USE_DEBUG_INFO:-COLUMNS}" \
		-e SDB_DEV="${TESTS_SDB_DEV:-On}" \
		-e SDB_FAULT_INJECTION="${TESTS_SDB_FAULT_INJECTION:-On}" \
		-e USE_IPO="${TESTS_USE_IPO:-Off}" \
		-e USE_COVERAGE="${TESTS_USE_COVERAGE:-}" \
		-e ENSURE_VTUNE_SYMBOLS="${TESTS_ENSURE_VTUNE_SYMBOLS:-Off}" \
		-e BUILD_DIR="${TESTS_BUILD_DIR:-build_tests}" \
		-v "${WORKSPACE}:/serenedb" \
		-v /mnt/data/.ccache:/.ccache \
		"${BUILD_IMAGE}" /serenedb/scripts/ci/build.bash serenedb tzdata
fi

mkdir -p cores
