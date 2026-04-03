#!/bin/bash

# Runs all sqllogic test suites in order, stopping on first failure.
# Usage:
#   ./run_all_tests.sh --host localhost --single-port 7777
#   ./run_all_tests.sh --host localhost --single-port 7777 --single-port-ssl 7778

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

declare -a SUITE_NAMES=()
declare -a SUITE_RESULTS=()
declare -a SUITE_TIMES=()
HAS_FAILURES=false

run_suite() {
	local name="$1"
	shift
	SUITE_NAMES+=("$name")
	echo "=== Running $name ==="
	local start=$(date +%s)
	"$@"
	local rc=$?
	local elapsed=$(($(date +%s) - start))
	SUITE_TIMES+=("${elapsed}s")
	if [[ $rc -eq 0 ]]; then
		SUITE_RESULTS+=("PASSED")
	else
		SUITE_RESULTS+=("FAILED")
		HAS_FAILURES=true
	fi
	echo ""
	return $rc
}

run_suite "SereneDB tests" \
	./run_sdb_tests.sh "$@" || true

run_suite "Cancellation tests" \
	./run_cancellation_tests.sh "$@" || true

run_suite "Recovery tests" \
	./run_recovery_tests.sh "$@" || true

# Print summary
echo "========================================"
echo "  TEST RESULTS SUMMARY"
echo "========================================"
for i in "${!SUITE_NAMES[@]}"; do
	printf "  %-8s %s (%s)\n" "${SUITE_RESULTS[$i]}" "${SUITE_NAMES[$i]}" "${SUITE_TIMES[$i]}"
done
if [[ "$HAS_FAILURES" == "true" ]]; then
	echo "========================================"
	echo "  SOME TESTS FAILED"
	echo "========================================"
	exit 1
fi
echo "========================================"
echo "  ALL TESTS PASSED"
echo "========================================"
