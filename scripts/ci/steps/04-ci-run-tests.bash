#!/bin/bash
# Runs selected test suites in parallel using GNU Parallel.
# Expects environment variables: VPACK_TESTS, IRESEARCH_TESTS, UNIT_TESTS, SQLLOGIC_TESTS, INTEGRATION_TESTS, RECOVERY_TESTS

set -e

# Create sanitizers dir if needed
if [[ -n "$SANITIZERS" ]]; then
	mkdir -p "$WORKSPACE/sanitizers"
fi

# Build array of test scripts to run in parallel
SCRIPTS=()
[[ "${VPACK_TESTS:-true}" == "true" ]] && SCRIPTS+=("BUILD_DIR=build_gtest ./scripts/ci/steps/041-ci-in-docker-run-vpack-tests.bash")
[[ "${IRESEARCH_TESTS:-true}" == "true" ]] && SCRIPTS+=("BUILD_DIR=build_gtest ./scripts/ci/steps/042-ci-in-docker-run-iresearch-tests.bash")
[[ "${UNIT_TESTS:-true}" == "true" ]] && SCRIPTS+=("BUILD_DIR=build_gtest ./scripts/ci/steps/043-ci-in-docker-run-serenedb-tests.bash")
# [[ "${SQLLOGIC_TESTS:-true}" == "true" ]] && SCRIPTS+=("BUILD_DIR=build_tests ./scripts/ci/steps/044-ci-in-docker-run-sqllogic-tests.bash")
# [[ "${RECOVERY_TESTS:-true}" == "true" ]] && SCRIPTS+=("BUILD_DIR=build_tests ./scripts/ci/steps/045-ci-in-docker-run-recovery-tests.bash")

if [[ ! ${#SCRIPTS[@]} -eq 0 ]]; then
	echo "Running ${#SCRIPTS[@]} test suite(s) in parallel:"
	printf '%s\n' "${SCRIPTS[@]}"

	parallel --jobs 4 --tagstring '{/.}' --line-buffer --halt soon,fail=1 \
		'bash -c {}' \
		::: "${SCRIPTS[@]}"
fi

# Run SQLLOGIC_TESTS & RECOVERY_TESTS separately
[[ "${SQLLOGIC_TESTS:-true}" == "true" ]] && BUILD_DIR=build_tests ./scripts/ci/steps/044-ci-in-docker-run-sqllogic-tests.bash
[[ "${RECOVERY_TESTS:-true}" == "true" ]] && BUILD_DIR=build_tests ./scripts/ci/steps/045-ci-in-docker-run-recovery-tests.bash

TEST_VALS=("$VPACK_TESTS" "$IRESEARCH_TESTS" "$UNIT_TESTS" "$SQLLOGIC_TESTS" "$RECOVERY_TESTS")
all_false=true
for val in "${TEST_VALS[@]}"; do
	[[ "$val" == "false" ]] || all_false=false
done
if [[ "$all_false" == "true" ]]; then
	echo "No tests selected, skipping."
fi
