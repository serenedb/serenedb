#!/bin/bash
# Ruby / pg (ruby-pg) driver harness.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR"

if ! command -v ruby >/dev/null 2>&1; then
	echo "[ruby] ruby not found" >&2
	exit 1
fi
if ! ruby -e 'require "pg"; require "yaml"' 2>/dev/null; then
	echo "[ruby] the pg gem is missing; gem install pg" >&2
	exit 1
fi

JUNIT="${SDB_DRV_JUNIT:-./out/drivers-tests}"
mkdir -p "$JUNIT"

ruby harness.rb "$JUNIT/tests-drivers-ruby-junit.xml"
