#!/bin/bash

# Cancellation stress test for sqllogic tests.
# Runs sqllogic tests and sends SIGINT at random intervals to verify
# that the server handles cancellation gracefully without crashes or corruption.
#
# Usage:
#   ./run_cancellation.sh [--iterations N] [--min-delay SECS] [--max-delay SECS]
#
# Environment:
#   SERVICE_HOST  - host where serenedb is running (default: serenedb-single)
#   SERVICE_PORT  - port for serenedb (default: 7777)

set -euo pipefail

ITERATIONS=${ITERATIONS:-10}
MIN_DELAY=${MIN_DELAY:-2}
MAX_DELAY=${MAX_DELAY:-30}

# Parse arguments
while [[ $# -gt 0 ]]; do
	case "$1" in
	--iterations)
		ITERATIONS="$2"
		shift 2
		;;
	--min-delay)
		MIN_DELAY="$2"
		shift 2
		;;
	--max-delay)
		MAX_DELAY="$2"
		shift 2
		;;
	*)
		echo "Unknown option: $1" >&2
		exit 1
		;;
	esac
done

SERVICE_HOST="${SERVICE_HOST:-serenedb-single}"
SERVICE_PORT="${SERVICE_PORT:-7777}"

cd "$(dirname "${BASH_SOURCE[0]}")"

echo "=== Cancellation stress test ==="
echo "Iterations: $ITERATIONS"
echo "SIGINT delay range: ${MIN_DELAY}s - ${MAX_DELAY}s"
echo "Host: $SERVICE_HOST:$SERVICE_PORT"
echo

random_delay() {
	local range=$((MAX_DELAY - MIN_DELAY))
	if [[ $range -le 0 ]]; then
		echo "$MIN_DELAY"
	else
		echo $(( (RANDOM % range) + MIN_DELAY ))
	fi
}

wait_for_server() {
	echo "Waiting for server to be ready..."
	for i in $(seq 1 60); do
		if bash -c "echo > /dev/tcp/${SERVICE_HOST}/${SERVICE_PORT}" 2>/dev/null; then
			echo "Server is ready."
			return 0
		fi
		sleep 1
	done
	echo "ERROR: Server not ready after 60 seconds"
	return 1
}

final_exit_code=0

for i in $(seq 1 "$ITERATIONS"); do
	echo "=========================================="
	echo "Iteration $i / $ITERATIONS"
	echo "=========================================="

	wait_for_server || { final_exit_code=1; break; }

	delay=$(random_delay)
	echo "Will send SIGINT after ${delay}s"

	# Start sqllogic tests in the background
	./run.sh \
		--host "$SERVICE_HOST" \
		--single-port "$SERVICE_PORT" \
		--test "sdb/**/*.test*" \
		--junit "tests-cancellation-$i" \
		--protocol simple \
		--runner="${RUNNER:-/sqllogictest-rs}" &
	test_pid=$!

	# Wait for the random delay, then send SIGINT
	sleep "$delay"

	if kill -0 "$test_pid" 2>/dev/null; then
		echo "Sending SIGINT to test runner (pid=$test_pid)..."
		kill -INT "$test_pid" 2>/dev/null || true
	else
		echo "Test runner already finished before SIGINT"
	fi

	# Wait for the test process to finish
	wait "$test_pid" || true
	echo "Test runner exited after SIGINT"
	echo

	# Verify server is still alive after cancellation
	echo "Checking server health after cancellation..."
	if ! wait_for_server; then
		echo "ERROR: Server is not responding after cancellation in iteration $i!"
		final_exit_code=1
		break
	fi
	echo "Server is healthy after iteration $i"
	echo
done

echo "=========================================="
if [[ $final_exit_code -eq 0 ]]; then
	echo "Cancellation stress test PASSED ($ITERATIONS iterations)"
else
	echo "Cancellation stress test FAILED"
fi
echo "=========================================="

exit $final_exit_code
