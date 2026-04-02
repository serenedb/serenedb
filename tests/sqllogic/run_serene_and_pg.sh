#!/bin/bash

exit_code=0

cd sqllogic

echo "Running tests against SereneDB"
./run.sh \
	--host serenedb-single \
	--single-port 7777 \
	--single-port-ssl 7778 \
	--test "sdb/**/*.test*" \
	--junit "tests-serenedb" \
	--protocol both \
	--runner=/sqllogictest-rs || exit_code=$?

if [[ $exit_code != 0 ]]; then
	echo "SereneDB tests failed, skipping PostgreSQL tests"
	exit $exit_code
fi

echo "Running tests against PostgreSQL"
./run.sh \
	--host postgres \
	--single-port 5432 \
	--test "pg/**/*.test*" \
	--junit "tests-postgres" \
	--protocol both \
	--database postgres \
	--runner=/sqllogictest-rs || exit_code=$?

if [[ $exit_code != 0 ]]; then
	echo "PostgreSQL tests failed, skipping cancellation tests"
	exit $exit_code
fi

# TODO: cancellation stress tests are disabled until the server properly handles
# SIGINT-based cancellation without becoming unresponsive. To re-enable, uncomment
# the block below and fix the server-side cancellation handling.
#
# echo "Running cancellation stress tests against SereneDB"
# ./run.sh \
# 	--host serenedb-single \
# 	--single-port 7777 \
# 	--single-port-ssl 7778 \
# 	--test "sdb/**/*.test*" \
# 	--junit "tests-cancellation" \
# 	--protocol simple \
# 	--runner=/sqllogictest-rs \
# 	--fast \
# 	--cancellation \
# 	--iterations 30 || exit_code=$?

exit $exit_code
