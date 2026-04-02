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
SQLLOGIC_RC=0
SQLLOGIC_TIME=0
RECOVERY_RC=0
RECOVERY_TIME=0
if [[ $PARALLEL_RC -eq 0 ]]; then
	if [[ "${SQLLOGIC_TESTS:-true}" == "true" ]]; then
		SQLLOGIC_START=$(date +%s)
		BUILD_DIR=build_tests ./scripts/ci/steps/044-ci-in-docker-run-sqllogic-tests.bash || SQLLOGIC_RC=$?
		SQLLOGIC_TIME=$(($(date +%s) - SQLLOGIC_START))
	fi
	if [[ $SQLLOGIC_RC -eq 0 && "${RECOVERY_TESTS:-true}" == "true" ]]; then
		RECOVERY_START=$(date +%s)
		BUILD_DIR=build_tests ./scripts/ci/steps/045-ci-in-docker-run-recovery-tests.bash || RECOVERY_RC=$?
		RECOVERY_TIME=$(($(date +%s) - RECOVERY_START))
	fi
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
# Serial tests
if [[ "${SQLLOGIC_TESTS:-true}" == "true" ]]; then
	if [[ $PARALLEL_RC -ne 0 ]]; then
		echo "  SKIPPED sqllogic-tests (parallel tests failed)"
	elif [[ $SQLLOGIC_RC -eq 0 ]]; then
		echo "  PASSED  sqllogic-tests (${SQLLOGIC_TIME}s)"
	else
		echo "  FAILED  sqllogic-tests (${SQLLOGIC_TIME}s, exit code ${SQLLOGIC_RC})"
	fi
fi
if [[ "${RECOVERY_TESTS:-true}" == "true" ]]; then
	if [[ $PARALLEL_RC -ne 0 || $SQLLOGIC_RC -ne 0 ]]; then
		echo "  SKIPPED recovery-tests (previous tests failed)"
	elif [[ $RECOVERY_RC -eq 0 ]]; then
		echo "  PASSED  recovery-tests (${RECOVERY_TIME}s)"
	else
		echo "  FAILED  recovery-tests (${RECOVERY_TIME}s, exit code ${RECOVERY_RC})"
	fi
fi
if [[ $PARALLEL_RC -ne 0 || $SQLLOGIC_RC -ne 0 || $RECOVERY_RC -ne 0 ]]; then
	echo "========================================"
	echo "  SOME TESTS FAILED"
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
