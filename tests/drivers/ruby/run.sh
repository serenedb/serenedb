#!/bin/bash
# Ruby harness: ruby-pg type matrix, then the ActiveRecord and Sequel suites.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

if ! command -v ruby >/dev/null 2>&1; then
	echo "[ruby] ruby not found" >&2
	exit 1
fi
for lib in pg yaml active_record sequel minitest; do
	if ! ruby -e "require '$lib'" 2>/dev/null; then
		echo "[ruby] $lib is missing; install the gems of $SCRIPT_DIR/Gemfile.lock" >&2
		exit 1
	fi
done

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"
export SDB_DRV_JUNIT="$JUNIT"

final=0
ruby harness.rb "$JUNIT/tests-drivers-ruby-junit.xml" || final=1
for suite in activerecord_test sequel_test; do
	echo "[ruby][$suite] running"
	ruby "$suite.rb" || final=1
done
exit "$final"
