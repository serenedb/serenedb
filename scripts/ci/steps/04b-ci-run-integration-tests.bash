#!/bin/bash
# Runs integration tests (sqllogic + recovery + drivers).
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
DRIVER_RC=0
DRIVER_TIME=0
EXTENSION_RC=0
EXTENSION_TIME=0

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

if [[ $SQLLOGIC_RC -eq 0 && $RECOVERY_RC -eq 0 && "${DRIVER_TESTS:-true}" == "true" ]]; then
	DRIVER_START=$(date +%s)
	BUILD_DIR="$BUILD_DIR" ./scripts/ci/steps/047-ci-in-docker-run-driver-tests.bash || DRIVER_RC=$?
	DRIVER_TIME=$(($(date +%s) - DRIVER_START))
fi

if [[ $SQLLOGIC_RC -eq 0 && $RECOVERY_RC -eq 0 && $DRIVER_RC -eq 0 && "${EXTENSION_TESTS:-true}" == "true" ]]; then
	EXTENSION_START=$(date +%s)
	BUILD_DIR="$BUILD_DIR" ./scripts/ci/steps/048-ci-in-docker-run-extension-tests.bash || EXTENSION_RC=$?
	EXTENSION_TIME=$(($(date +%s) - EXTENSION_START))
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
if [[ "${DRIVER_TESTS:-true}" == "true" ]]; then
	if [[ $SQLLOGIC_RC -ne 0 ]]; then
		echo "  SKIPPED driver-tests (sqllogic failed)"
	elif [[ $RECOVERY_RC -ne 0 ]]; then
		echo "  SKIPPED driver-tests (recovery failed)"
	elif [[ $DRIVER_RC -eq 0 ]]; then
		echo "  PASSED  driver-tests (${DRIVER_TIME}s)"
	else
		echo "  FAILED  driver-tests (${DRIVER_TIME}s, exit code ${DRIVER_RC})"
	fi
fi
if [[ "${EXTENSION_TESTS:-true}" == "true" ]]; then
	if [[ $SQLLOGIC_RC -ne 0 ]]; then
		echo "  SKIPPED extension-tests (sqllogic failed)"
	elif [[ $RECOVERY_RC -ne 0 ]]; then
		echo "  SKIPPED extension-tests (recovery failed)"
	elif [[ $DRIVER_RC -ne 0 ]]; then
		echo "  SKIPPED extension-tests (driver failed)"
	elif [[ $EXTENSION_RC -eq 0 ]]; then
		echo "  PASSED  extension-tests (${EXTENSION_TIME}s)"
	else
		echo "  FAILED  extension-tests (${EXTENSION_TIME}s, exit code ${EXTENSION_RC})"
	fi
fi
if [[ $SQLLOGIC_RC -ne 0 || $RECOVERY_RC -ne 0 || $DRIVER_RC -ne 0 || $EXTENSION_RC -ne 0 ]]; then
	echo "========================================"
	echo "  SOME INTEGRATION TESTS FAILED"
	echo "========================================"
	exit 1
fi
echo "========================================"
echo "  ALL INTEGRATION TESTS PASSED"
echo "========================================"
