#!/bin/bash

# Runs recovery tests against a running serened instance.
# Expects serened to auto-restart on crash (e.g. via run_serened_loop.sh).
#
# Usage (local dev, from another terminal):
#   ./run_recovery.sh
#   SERVICE_HOST=serenedb-recovery SERVICE_PORT=7777 ./run_recovery.sh

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

: "${SERVICE_HOST:=localhost}"
: "${SERVICE_PORT:=7777}"

cd "$SCRIPT_DIR"

export RETRY_ATTEMPTS=10
export BACKOFF_DURATION=500ms

declare -a test_files=()
while IFS= read -r -d '' file; do
	test_files+=("${file#./}")
done < <(find recovery/ -name "*.test" -type f -print0 | sort -z)

if [[ ${#test_files[@]} -eq 0 ]]; then
	echo "No test files found in recovery/ directory"
	exit 1
fi

echo "Found ${#test_files[@]} recovery test(s)"

final_exit_code=0

for test_file in "${test_files[@]}"; do
	echo "Running recovery test: $test_file"

	./run.sh \
		--host "$SERVICE_HOST" \
		--single-port "$SERVICE_PORT" \
		--test "$test_file" \
		--junit "tests-serenedb-recovery" \
		--protocol simple \
		--runner=/sqllogictest-rs

	exit_code=$?

	if [[ $exit_code != 0 ]]; then
		echo "SereneDB recovery test failed: $test_file"
		final_exit_code=$exit_code
	fi
done

if [[ $final_exit_code != 0 ]]; then
	echo "SereneDB recovery tests failed"
	exit $final_exit_code
fi

echo "All recovery tests passed"
exit 0
