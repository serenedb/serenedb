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

JOBLOG=$(mktemp)
PARALLEL_RC=0
if [[ ${#SCRIPTS[@]} -gt 0 ]]; then
	echo "Running ${#SCRIPTS[@]} test suite(s) in parallel:"
	printf '%s\n' "${SCRIPTS[@]}"

	parallel --jobs 4 --tagstring '{/.}' --line-buffer --joblog "$JOBLOG" --halt now,fail=1 \
		'bash -c {}' \
		::: "${SCRIPTS[@]}" || PARALLEL_RC=$?
fi

# Run SQLLOGIC_TESTS & RECOVERY_TESTS separately
SERIAL_RC=0
if [[ $PARALLEL_RC -eq 0 ]]; then
	[[ "${SQLLOGIC_TESTS:-true}" == "true" ]] && { BUILD_DIR=build_tests ./scripts/ci/steps/044-ci-in-docker-run-sqllogic-tests.bash || SERIAL_RC=$?; }
	[[ $SERIAL_RC -eq 0 && "${RECOVERY_TESTS:-true}" == "true" ]] && { BUILD_DIR=build_tests ./scripts/ci/steps/045-ci-in-docker-run-recovery-tests.bash || SERIAL_RC=$?; }
fi

# Print summary
echo ""
echo "========================================"
echo "  TEST RESULTS SUMMARY"
echo "========================================"
if [[ -s "$JOBLOG" ]]; then
	while IFS=$'\t' read -r seq host starttime jobruntime send receive exitval signal command; do
		[[ "$seq" == "Seq" ]] && continue
		name=$(basename "$command" .bash | sed 's/^[0-9]*-ci-in-docker-run-//')
		if [[ "$exitval" -eq 0 ]]; then
			echo "  PASSED  ${name} (${jobruntime}s)"
		elif [[ "$signal" -ne 0 ]]; then
			echo "  KILLED  ${name} (signal ${signal})"
		else
			echo "  FAILED  ${name} (${jobruntime}s, exit code ${exitval})"
		fi
	done <"$JOBLOG"
fi
rm -f "$JOBLOG"
if [[ $PARALLEL_RC -ne 0 || $SERIAL_RC -ne 0 ]]; then
	echo "========================================"
	echo "  SOME TESTS FAILED (see above)"
	echo "========================================"
	exit 1
fi
echo "========================================"
echo "  ALL TESTS PASSED"
echo "========================================"

TEST_VALS=("$VPACK_TESTS" "$IRESEARCH_TESTS" "$UNIT_TESTS" "$SQLLOGIC_TESTS" "$RECOVERY_TESTS")
all_false=true
for val in "${TEST_VALS[@]}"; do
	[[ "$val" == "false" ]] || all_false=false
done
if [[ "$all_false" == "true" ]]; then
	echo "No tests selected, skipping."
fi
