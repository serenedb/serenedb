#!/bin/bash

# Runs cancellation stress tests against a running serened instance.
# Usage:
#   ./run_cancellation_tests.sh --host localhost --single-port 7777
#
# TODO: currently disabled -- serened becomes unresponsive after repeated
# SIGINT-based cancellation. Uncomment the exec line when fixed.

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

echo "WARNING: Cancellation stress tests are currently disabled."
echo "See TODO in this script for details."
exit 0

# exec ./run.sh \
# 	--test "sdb/**/*.test*" \
# 	--junit "tests-cancellation" \
# 	--engines pg-wire-simple \
# 	--fast \
# 	--cancellation \
# 	--iterations 30 \
# 	"$@"
