#!/bin/bash
# Runs integration tests (sqllogic + recovery).
# Expects: BUILD_DIR, WORKSPACE, BUILD_IMAGE, CARGO_TARGET_CACHE

set -e

if [[ -n "$SANITIZERS" ]]; then
	mkdir -p "$WORKSPACE/sanitizers"
fi

: "${BUILD_DIR:=build_tests}"

SQLLOGIC_RC=0
SQLLOGIC_TIME=0
RECOVERY_RC=0
RECOVERY_TIME=0

if [[ "${SQLLOGIC_TESTS:-true}" == "true" ]]; then
	SQLLOGIC_START=$(date +%s)
	BUILD_DIR="$BUILD_DIR" ./scripts/ci/steps/044-ci-in-docker-run-sqllogic-tests.bash || SQLLOGIC_RC=$?
	SQLLOGIC_TIME=$(($(date +%s) - SQLLOGIC_START))
fi

if [[ $SQLLOGIC_RC -eq 0 && "${RECOVERY_TESTS:-true}" == "true" ]]; then
	RECOVERY_START=$(date +%s)
	BUILD_DIR="$BUILD_DIR" ./scripts/ci/steps/045-ci-in-docker-run-recovery-tests.bash || RECOVERY_RC=$?
	RECOVERY_TIME=$(($(date +%s) - RECOVERY_START))
fi

# Print summary
echo ""
echo "========================================"
echo "  INTEGRATION TEST RESULTS SUMMARY"
echo "========================================"
if [[ "${SQLLOGIC_TESTS:-true}" == "true" ]]; then
	if [[ $SQLLOGIC_RC -eq 0 ]]; then
		echo "  PASSED  sqllogic-tests (${SQLLOGIC_TIME}s)"
	else
		echo "  FAILED  sqllogic-tests (${SQLLOGIC_TIME}s, exit code ${SQLLOGIC_RC})"
	fi
fi
if [[ "${RECOVERY_TESTS:-true}" == "true" ]]; then
	if [[ $SQLLOGIC_RC -ne 0 ]]; then
		echo "  SKIPPED recovery-tests (sqllogic failed)"
	elif [[ $RECOVERY_RC -eq 0 ]]; then
		echo "  PASSED  recovery-tests (${RECOVERY_TIME}s)"
	else
		echo "  FAILED  recovery-tests (${RECOVERY_TIME}s, exit code ${RECOVERY_RC})"
	fi
fi
if [[ $SQLLOGIC_RC -ne 0 || $RECOVERY_RC -ne 0 ]]; then
	echo "========================================"
	echo "  SOME INTEGRATION TESTS FAILED"
	echo "========================================"
	exit 1
fi
echo "========================================"
echo "  ALL INTEGRATION TESTS PASSED"
echo "========================================"
